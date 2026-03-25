from __future__ import annotations

import time
import traceback
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_pinball_loss, mean_squared_error
from tqdm.auto import tqdm

from src.model_a import DEFAULT_QUANTILES, FuelDecisionModelA, WaitBuyDecisionPolicy


@dataclass(slots=True)
class CalibrationRun:
    results: pd.DataFrame
    failures: pd.DataFrame


def temporal_split(eligible: pd.DataFrame, train_end_date: str | None) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    if eligible.empty:
        raise ValueError("No hay filas elegibles tras filtrar observaciones y target disponible.")

    if train_end_date:
        cutoff = pd.Timestamp(train_end_date)
    else:
        unique_dates = sorted(pd.to_datetime(eligible["date"]).dropna().unique())
        cutoff = pd.Timestamp(unique_dates[int(len(unique_dates) * 0.8)])

    train_df = eligible[eligible["date"] <= cutoff].copy()
    test_df = eligible[eligible["date"] > cutoff].copy()
    if train_df.empty or test_df.empty:
        raise ValueError("El split temporal quedó vacío. Ajusta train_end_date u horizonte.")
    return train_df, test_df, cutoff


def evaluate_predictions(df: pd.DataFrame, target_column: str) -> dict[str, float]:
    y_true = df[target_column]
    metrics = {
        "mae_q50": float(mean_absolute_error(y_true, df["pred_q50"])),
        "rmse_q50": float(mean_squared_error(y_true, df["pred_q50"], squared=False)),
    }
    for q in DEFAULT_QUANTILES:
        col = f"pred_q{int(q*100):02d}"
        metrics[f"mae_q{int(q*100):02d}"] = float(mean_absolute_error(y_true, df[col]))
        metrics[f"pinball_q{int(q*100):02d}"] = float(mean_pinball_loss(y_true, df[col], alpha=q))
    return metrics


def fit_one_horizon(panel_df: pd.DataFrame, fuel_col: str, horizon_days: int, train_end_date: str | None) -> dict[str, Any]:
    t0 = time.perf_counter()
    model = FuelDecisionModelA(
        fuel_col=fuel_col,
        horizon_days=horizon_days,
        train_end_date=train_end_date,
        waiting_cost=0.003,
    )
    frame = model.build_training_frame(panel_df)
    target_col = f"target_min_{horizon_days}d"
    required_cols = ["is_observed_price", "target_available", "price", target_col]
    missing = [c for c in required_cols if c not in frame.columns]
    if missing:
        raise KeyError(f"Faltan columnas requeridas: {missing}")

    eligible = frame[(frame["is_observed_price"] == 1) & (frame["target_available"] == 1)].copy()
    eligible = eligible.dropna(subset=["price"])
    train_df, test_df, cutoff = temporal_split(eligible, train_end_date)

    model.forecaster.fit(train_df, feature_columns=model.feature_columns_, target_column=target_col)
    pred_test = model.forecaster.predict(test_df.copy())
    pred_latest = model.forecaster.predict(
        frame[(frame["date"] == frame["date"].max()) & (frame["is_observed_price"] == 1)].copy()
    )

    return {
        "model": model,
        "frame": frame,
        "target_col": target_col,
        "cutoff": cutoff,
        "train_df": train_df,
        "test_df": test_df,
        "pred_test": pred_test,
        "pred_latest": pred_latest,
        "forecast_metrics": evaluate_predictions(pred_test, target_col),
        "fit_elapsed_seconds": time.perf_counter() - t0,
    }


def evaluate_waiting_costs(pred_test: pd.DataFrame, pred_latest: pd.DataFrame, target_col: str, waiting_cost_candidates: list[float], horizon_days: int, show_progress: bool = True) -> tuple[pd.DataFrame, dict[float, pd.DataFrame]]:
    rows = []
    latest_outputs: dict[float, pd.DataFrame] = {}
    base_test_cols = ["date", "station_id", "price", "regime", "pred_q20", "pred_q50", "pred_q80", target_col]
    base_latest_cols = ["date", "station_id", "price", "regime", "pred_q20", "pred_q50", "pred_q80"]

    iterator = waiting_cost_candidates
    if show_progress:
        iterator = tqdm(waiting_cost_candidates, desc=f"waiting_cost H={horizon_days}", unit="cfg", leave=False)

    test_eval = pred_test[base_test_cols].copy()
    latest_eval = pred_latest[base_latest_cols].copy()
    for waiting_cost in iterator:
        policy = WaitBuyDecisionPolicy(waiting_cost=waiting_cost)
        latest_outputs[waiting_cost] = policy.apply(latest_eval.assign(**{target_col: np.nan}).copy())
        summary = policy.summarize(test_eval.copy())
        rows.append({"horizon_days": horizon_days, "waiting_cost": waiting_cost, **summary})
    return pd.DataFrame(rows), latest_outputs


def calibrate_model_a(panel_df: pd.DataFrame, fuel_col: str, horizon_candidates: list[int], waiting_cost_candidates: list[float], train_end_date: str | None, show_progress: bool = True, continue_on_error: bool = True) -> CalibrationRun:
    calibration_rows = []
    failures = []
    iterator = horizon_candidates
    if show_progress:
        iterator = tqdm(horizon_candidates, desc="Calibrando horizon_days", unit="h")

    for horizon_days in iterator:
        try:
            artifact = fit_one_horizon(panel_df, fuel_col, horizon_days, train_end_date)
            policy_df, _ = evaluate_waiting_costs(
                artifact["pred_test"], artifact["pred_latest"], artifact["target_col"], waiting_cost_candidates, horizon_days, show_progress=show_progress
            )
            for metric_name, metric_value in artifact["forecast_metrics"].items():
                policy_df[metric_name] = metric_value
            policy_df["rows_train"] = len(artifact["train_df"])
            policy_df["rows_test"] = len(artifact["test_df"])
            policy_df["train_end_date"] = str(artifact["cutoff"].date())
            policy_df["fit_elapsed_seconds"] = artifact["fit_elapsed_seconds"]
            calibration_rows.append(policy_df)
        except Exception as exc:
            failures.append({
                "horizon_days": horizon_days,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            })
            if not continue_on_error:
                raise

    results = pd.concat(calibration_rows, ignore_index=True) if calibration_rows else pd.DataFrame()
    return CalibrationRun(results=results, failures=pd.DataFrame(failures))


def generate_rolling_folds(dates: pd.Series, min_train_days: int, test_window_days: int, step_days: int, max_folds: int | None = None) -> list[dict[str, Any]]:
    unique_dates = pd.Series(pd.to_datetime(dates).dropna().unique()).sort_values().reset_index(drop=True)
    folds = []
    start_idx = min_train_days
    while start_idx < len(unique_dates):
        train_end = unique_dates.iloc[start_idx - 1]
        test_start = unique_dates.iloc[start_idx]
        test_end = test_start + pd.Timedelta(days=test_window_days - 1)
        available_test_dates = unique_dates[(unique_dates >= test_start) & (unique_dates <= test_end)]
        if available_test_dates.empty:
            break
        folds.append({
            "train_end": pd.Timestamp(train_end),
            "test_start": pd.Timestamp(test_start),
            "test_end": pd.Timestamp(available_test_dates.max()),
        })
        start_idx += step_days
        if max_folds is not None and len(folds) >= max_folds:
            break
    return folds


def run_rolling_backtest(panel_df: pd.DataFrame, fuel_col: str, horizon_candidates: list[int], waiting_cost_candidates: list[float], min_train_days: int, test_window_days: int, step_days: int, max_folds: int | None = None, show_progress: bool = True, continue_on_error: bool = True) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    folds = generate_rolling_folds(panel_df["date"], min_train_days, test_window_days, step_days, max_folds)
    combos = [(h, idx + 1, fold) for h in horizon_candidates for idx, fold in enumerate(folds)]
    rows_folds = []
    rows_policy = []
    failures = []
    iterator = combos
    if show_progress:
        iterator = tqdm(combos, desc="Rolling backtest", unit="fold")

    for horizon_days, fold_id, fold in iterator:
        try:
            artifact = fit_one_horizon(
                panel_df[pd.to_datetime(panel_df["date"]) <= fold["test_end"]].copy(),
                fuel_col,
                horizon_days,
                str(fold["train_end"].date()),
            )
            pred_test = artifact["pred_test"].copy()
            pred_test["date"] = pd.to_datetime(pred_test["date"])
            pred_test = pred_test[(pred_test["date"] >= fold["test_start"]) & (pred_test["date"] <= fold["test_end"])].copy()
            if pred_test.empty:
                continue
            fold_metrics = {
                "horizon_days": horizon_days,
                "fold_id": fold_id,
                "cutoff_date": str(fold["train_end"].date()),
                "test_end_date": str(fold["test_end"].date()),
                "rows_train": len(artifact["train_df"]),
                "rows_test": len(pred_test),
                "fit_elapsed_seconds": artifact["fit_elapsed_seconds"],
                **evaluate_predictions(pred_test, artifact["target_col"]),
            }
            rows_folds.append(fold_metrics)
            for waiting_cost in waiting_cost_candidates:
                policy = WaitBuyDecisionPolicy(waiting_cost=waiting_cost)
                summary = policy.summarize(pred_test[["price", "regime", artifact["target_col"], "pred_q20", "pred_q50", "pred_q80"]].copy())
                rows_policy.append({
                    "horizon_days": horizon_days,
                    "fold_id": fold_id,
                    "cutoff_date": str(fold["train_end"].date()),
                    "waiting_cost": waiting_cost,
                    **summary,
                })
        except Exception as exc:
            failures.append({
                "horizon_days": horizon_days,
                "fold_id": fold_id,
                "cutoff_date": str(fold["train_end"].date()),
                "test_end_date": str(fold["test_end"].date()),
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            })
            if not continue_on_error:
                raise

    folds_df = pd.DataFrame(rows_folds)
    policy_df = pd.DataFrame(rows_policy)
    failures_df = pd.DataFrame(failures)
    if policy_df.empty or folds_df.empty:
        return pd.DataFrame(), folds_df, failures_df
    rolling_summary = (
        policy_df.groupby(["horizon_days", "waiting_cost"], as_index=False)
        .agg(
            folds=("fold_id", "nunique"),
            avg_policy_realized_saving=("avg_policy_realized_saving", "mean"),
            median_policy_realized_saving=("avg_policy_realized_saving", "median"),
            decision_accuracy_vs_oracle=("decision_accuracy_vs_oracle", "mean"),
            share_wait=("share_wait", "mean"),
            avg_realized_wait_gain=("avg_realized_wait_gain", "mean"),
        )
    )
    fold_means = (
        folds_df.groupby("horizon_days", as_index=False)
        .agg(
            mae_q20=("mae_q20", "mean"),
            mae_q50=("mae_q50", "mean"),
            mae_q80=("mae_q80", "mean"),
            rmse_q50=("rmse_q50", "mean"),
            rows_train_mean=("rows_train", "mean"),
            rows_test_mean=("rows_test", "mean"),
            fit_elapsed_seconds_mean=("fit_elapsed_seconds", "mean"),
        )
    )
    rolling_summary = rolling_summary.merge(fold_means, on="horizon_days", how="left")
    rolling_summary = rolling_summary.sort_values(["avg_policy_realized_saving", "decision_accuracy_vs_oracle"], ascending=[False, False]).reset_index(drop=True)
    return rolling_summary, folds_df, failures_df if not failures_df.empty else failures_df
