from __future__ import annotations

"""
Modelo A: detector de régimen + forecast cuantílico del mínimo en horizonte D
+ regla de decisión esperar/repostar.

Diseñado para uso desde notebook o scripts, sin depender del LSTM.
"""

from dataclasses import dataclass
from pathlib import Path
import json
import logging
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_pinball_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from pandas.api.types import (
    is_bool_dtype,
    is_categorical_dtype,
    is_numeric_dtype,
    is_object_dtype,
    is_string_dtype,
)

from src.database import PRICE_COLUMNS
from src.dataset_builder import FuelDatasetBuilder

logger = logging.getLogger(__name__)


DEFAULT_QUANTILES = (0.2, 0.5, 0.8)
DEFAULT_MIN_TRAIN_ROWS_PER_REGIME = 150


def _ensure_panel_schema(panel_df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza el esquema del panel para soportar notebooks y versiones distintas de pandas."""
    df = panel_df.copy()

    if "station_id" not in df.columns:
        if df.index.name == "station_id":
            df = df.reset_index()
        else:
            fallback_map = {
                "id": "station_id",
                "id_estacion": "station_id",
                "station": "station_id",
                "estacion": "station_id",
            }
            for src, dst in fallback_map.items():
                if src in df.columns and dst not in df.columns:
                    df = df.rename(columns={src: dst})
                    break

    if "date" not in df.columns:
        if df.index.name == "date":
            df = df.reset_index()
        elif isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={df.index.name or "index": "date"})

    return df


@dataclass(slots=True)
class BacktestResult:
    horizon_days: int
    rows_train: int
    rows_test: int
    metrics: dict
    policy_summary: dict

    def to_dict(self) -> dict:
        return {
            "horizon_days": self.horizon_days,
            "rows_train": self.rows_train,
            "rows_test": self.rows_test,
            "metrics": self.metrics,
            "policy_summary": self.policy_summary,
        }


class RegimeDetector:
    """Detección de régimen basada en reglas robustas, no en ML."""

    def __init__(
        self,
        min_reset_jump: float = 0.015,
        shock_abs_threshold: float = 0.025,
        cyclical_resets_window: int = 90,
        min_resets_for_cyclical: int = 2,
    ) -> None:
        self.min_reset_jump = float(min_reset_jump)
        self.shock_abs_threshold = float(shock_abs_threshold)
        self.cyclical_resets_window = int(cyclical_resets_window)
        self.min_resets_for_cyclical = int(min_resets_for_cyclical)

    def transform(self, df: pd.DataFrame, price_col: str = "price") -> pd.DataFrame:
        work = _ensure_panel_schema(df).sort_values(["station_id", "date"]).copy()

        def _per_station(station_df: pd.DataFrame) -> pd.DataFrame:
            station_df = station_df.copy()
            price = station_df[price_col]
            delta_1 = price.diff()
            abs_delta = delta_1.abs()
            robust_scale = abs_delta.rolling(60, min_periods=10).median().fillna(abs_delta.median())
            reset_threshold = np.maximum(self.min_reset_jump, 3.0 * robust_scale)
            shock_threshold = np.maximum(self.shock_abs_threshold, 4.0 * robust_scale)

            station_df["delta_1"] = delta_1
            station_df["delta_3"] = price.diff(3)
            station_df["delta_7"] = price.diff(7)
            station_df["rolling_std_14"] = price.rolling(14, min_periods=5).std()
            station_df["rolling_mean_14"] = price.rolling(14, min_periods=5).mean()
            station_df["cycle_reset_flag"] = (delta_1 >= reset_threshold).astype(int)
            station_df["recent_resets_90"] = (
                station_df["cycle_reset_flag"]
                .rolling(self.cyclical_resets_window, min_periods=1)
                .sum()
            )

            last_reset_date = station_df["date"].where(station_df["cycle_reset_flag"] == 1)
            station_df["days_since_reset"] = (station_df["date"] - last_reset_date.ffill()).dt.days
            station_df["days_since_reset"] = station_df["days_since_reset"].fillna(999).astype(int)

            regime = np.where(
                station_df["delta_3"] >= shock_threshold,
                "shock_up",
                np.where(
                    station_df["delta_3"] <= -shock_threshold,
                    "shock_down",
                    np.where(
                        station_df["recent_resets_90"] >= self.min_resets_for_cyclical,
                        "cyclical",
                        "stable",
                    ),
                ),
            )
            station_df["regime"] = regime
            return station_df

        parts = []
        for station_id, station_df in work.groupby("station_id", sort=False, dropna=False):
            station_df = station_df.copy()
            station_df["station_id"] = station_id
            parts.append(_per_station(station_df))

        if not parts:
            return work.iloc[0:0].copy()
        return pd.concat(parts, ignore_index=True)


class FeatureBuilder:
    def __init__(self, fuel_col: str, horizon_days: int = 7, fill_limit: int = 3) -> None:
        if fuel_col not in PRICE_COLUMNS:
            raise ValueError(f"fuel_col desconocida: {fuel_col}")
        self.fuel_col = fuel_col
        self.horizon_days = int(horizon_days)
        self.fill_limit = int(fill_limit)
        self.regime_detector = RegimeDetector()

    def build(self, panel_df: pd.DataFrame) -> pd.DataFrame:
        if panel_df.empty:
            return panel_df.copy()

        df = _ensure_panel_schema(panel_df)
        required = {"date", "station_id", self.fuel_col}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Faltan columnas requeridas: {sorted(missing)}")

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(["station_id", "date"]).reset_index(drop=True)
        df["price"] = pd.to_numeric(df[self.fuel_col], errors="coerce")
        df["is_observed_price"] = df.get("is_observed_price", df["price"].notna().astype(int)).fillna(0).astype(int)

        df["price_ffill"] = (
            df.groupby("station_id")["price"]
            .transform(lambda s: s.ffill(limit=self.fill_limit))
        )
        df["days_since_observed"] = (
            df.groupby("station_id")["is_observed_price"]
            .transform(lambda s: s.eq(1).cumsum())
        )

        market = (
            df.groupby("date")["price"]
            .agg(market_min="min", market_mean="mean", market_median="median", market_std="std")
            .reset_index()
        )
        df = df.merge(market, on="date", how="left")

        df = self.regime_detector.transform(df, price_col="price_ffill")

        def _per_station(station_df: pd.DataFrame) -> pd.DataFrame:
            station_df = station_df.copy()
            s = station_df["price_ffill"]

            for lag in (1, 2, 3, 7, 14, 30):
                station_df[f"lag_{lag}"] = s.shift(lag)

            station_df["change_1"] = s - station_df["lag_1"]
            station_df["change_3"] = s - station_df["lag_3"]
            station_df["change_7"] = s - station_df["lag_7"]
            station_df["change_14"] = s - station_df["lag_14"]

            station_df["roll_mean_7"] = s.rolling(7, min_periods=3).mean()
            station_df["roll_std_7"] = s.rolling(7, min_periods=3).std()
            station_df["roll_mean_30"] = s.rolling(30, min_periods=10).mean()
            station_df["roll_std_30"] = s.rolling(30, min_periods=10).std()
            station_df["roll_min_365"] = s.rolling(365, min_periods=30).min()
            station_df["roll_max_365"] = s.rolling(365, min_periods=30).max()

            denom = station_df["roll_max_365"] - station_df["roll_min_365"]
            station_df["price_position_365"] = np.where(
                denom > 0,
                (s - station_df["roll_min_365"]) / denom,
                np.nan,
            )

            raw = station_df["price"]
            future_min = raw.shift(-1).iloc[::-1].rolling(self.horizon_days, min_periods=1).min().iloc[::-1]
            station_df[f"target_min_{self.horizon_days}d"] = future_min
            station_df[f"target_saving_{self.horizon_days}d"] = station_df["price"] - future_min
            return station_df

        parts = []
        for station_id, station_df in df.groupby("station_id", sort=False, dropna=False):
            station_df = station_df.copy()
            station_df["station_id"] = station_id
            parts.append(_per_station(station_df))

        if parts:
            df = pd.concat(parts, ignore_index=True)
        else:
            df = df.iloc[0:0].copy()

        df["spread_to_market_median"] = df["price_ffill"] - df["market_median"]
        df["spread_to_market_min"] = df["price_ffill"] - df["market_min"]
        df["price_rank_pct"] = (
            df.groupby("date")["price_ffill"]
            .rank(method="average", pct=True)
        )
        cheaper_count = (
            df.groupby("date")["price_ffill"]
            .transform(lambda s: s.rank(method="min") - 1)
        )
        df["n_cheaper_stations"] = cheaper_count

        df["day_of_week"] = df["date"].dt.dayofweek
        df["month"] = df["date"].dt.month
        df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7.0)
        df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7.0)
        df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12.0)
        df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12.0)

        df["target_available"] = df[f"target_min_{self.horizon_days}d"].notna().astype(int)
        return df


class HorizonMinForecaster:
    def __init__(
        self,
        horizon_days: int = 7,
        quantiles: tuple[float, ...] = DEFAULT_QUANTILES,
        min_train_rows_per_regime: int = DEFAULT_MIN_TRAIN_ROWS_PER_REGIME,
    ) -> None:
        self.horizon_days = int(horizon_days)
        self.quantiles = tuple(quantiles)
        self.min_train_rows_per_regime = int(min_train_rows_per_regime)
        self.feature_columns_: list[str] = []
        self.categorical_columns_: list[str] = []
        self.numeric_columns_: list[str] = []
        self.models_: dict[tuple[str, float], Pipeline] = {}
        self.global_models_: dict[float, Pipeline] = {}
        self.regimes_trained_: set[str] = set()

    @staticmethod
    def _is_categorical_feature(series: pd.Series) -> bool:
        dtype = series.dtype
        if is_bool_dtype(dtype):
            return False
        return bool(
            is_object_dtype(dtype)
            or is_string_dtype(dtype)
            or is_categorical_dtype(dtype)
        )

    def _infer_feature_types(self, df: pd.DataFrame, feature_columns: list[str]) -> tuple[list[str], list[str]]:
        categorical_columns: list[str] = []
        numeric_columns: list[str] = []
        for col in feature_columns:
            series = df[col]
            if self._is_categorical_feature(series):
                categorical_columns.append(col)
            elif is_numeric_dtype(series.dtype):
                numeric_columns.append(col)
            else:
                categorical_columns.append(col)
        return categorical_columns, numeric_columns

    def _prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        work = df[self.feature_columns_].copy()
        for col in self.categorical_columns_:
            if col in work.columns:
                work[col] = work[col].astype("string")
        for col in self.numeric_columns_:
            if col in work.columns:
                work[col] = pd.to_numeric(work[col], errors="coerce")
        return work

    def fit(self, df: pd.DataFrame, feature_columns: list[str], target_column: str) -> "HorizonMinForecaster":
        train_df = df.dropna(subset=[target_column]).copy()
        self.feature_columns_ = list(feature_columns)
        self.categorical_columns_, self.numeric_columns_ = self._infer_feature_types(train_df, self.feature_columns_)

        X_global = self._prepare_features(train_df)
        y_global = train_df[target_column]
        for q in self.quantiles:
            model = self._make_pipeline(q)
            model.fit(X_global, y_global)
            self.global_models_[q] = model

        for regime, regime_df in train_df.groupby("regime"):
            if len(regime_df) < self.min_train_rows_per_regime:
                continue
            self.regimes_trained_.add(regime)
            X_reg = self._prepare_features(regime_df)
            y_reg = regime_df[target_column]
            for q in self.quantiles:
                model = self._make_pipeline(q)
                model.fit(X_reg, y_reg)
                self.models_[(regime, q)] = model

        return self

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        work = df.copy()
        feature_df = self._prepare_features(work)
        for q in self.quantiles:
            preds = np.empty(len(work), dtype=float)
            for regime in work["regime"].fillna("stable").unique():
                mask = work["regime"].fillna("stable") == regime
                model = self.models_.get((regime, q), self.global_models_[q])
                preds[mask] = model.predict(feature_df.loc[mask, self.feature_columns_])
            work[f"pred_q{int(q*100):02d}"] = preds

        pred_cols = [f"pred_q{int(q*100):02d}" for q in self.quantiles]
        work[pred_cols] = np.sort(work[pred_cols].to_numpy(), axis=1)
        return work

    def _make_pipeline(self, quantile: float) -> Pipeline:
        preprocessor = ColumnTransformer(
            transformers=[
                (
                    "num",
                    Pipeline([
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]),
                    self.numeric_columns_,
                ),
                (
                    "cat",
                    Pipeline([
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]),
                    self.categorical_columns_,
                ),
            ]
        )
        loss = "quantile"
        alpha = quantile
        if abs(quantile - 0.5) < 1e-9:
            loss = "absolute_error"
            alpha = 0.5

        return Pipeline([
            ("preprocessor", preprocessor),
            (
                "regressor",
                GradientBoostingRegressor(
                    loss=loss,
                    alpha=alpha,
                    n_estimators=250,
                    learning_rate=0.05,
                    max_depth=3,
                    min_samples_leaf=10,
                    random_state=42,
                ),
            ),
        ])

    def save(self, path: str | Path) -> str:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self, path)
        return str(path)

    @staticmethod
    def load(path: str | Path) -> "HorizonMinForecaster":
        return joblib.load(path)


class WaitBuyDecisionPolicy:
    def __init__(self, waiting_cost: float = 0.003, conservative_quantile: float = 0.8) -> None:
        self.waiting_cost = float(waiting_cost)
        self.conservative_quantile = float(conservative_quantile)

    def decide_row(self, row: pd.Series) -> str:
        current_price = row["price"]
        pred_q20 = row.get("pred_q20")
        pred_q50 = row.get("pred_q50")
        pred_q80 = row.get("pred_q80")

        conservative_future_min = pred_q80 if pd.notna(pred_q80) else pred_q50
        central_future_min = pred_q50 if pd.notna(pred_q50) else pred_q20

        probable_gain = current_price - conservative_future_min
        expected_gain = current_price - central_future_min

        if probable_gain > self.waiting_cost:
            return "wait"
        if expected_gain <= self.waiting_cost:
            return "buy_now"
        if row.get("regime") == "cyclical" and expected_gain > self.waiting_cost / 2:
            return "wait"
        return "buy_now"

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        work = df.copy()
        work["decision"] = work.apply(self.decide_row, axis=1)
        work["realized_wait_gain"] = work["price"] - work.filter(regex=r"^target_min_\d+d$").iloc[:, 0]
        work["policy_realized_saving"] = np.where(
            work["decision"] == "wait",
            np.maximum(work["realized_wait_gain"] - self.waiting_cost, 0.0),
            0.0,
        )
        work["oracle_best_action"] = np.where(
            work["realized_wait_gain"] > self.waiting_cost,
            "wait",
            "buy_now",
        )
        return work

    def summarize(self, df: pd.DataFrame) -> dict:
        work = self.apply(df)
        if work.empty:
            return {}
        accuracy = float((work["decision"] == work["oracle_best_action"]).mean())
        return {
            "rows": int(len(work)),
            "decision_accuracy_vs_oracle": accuracy,
            "share_wait": float((work["decision"] == "wait").mean()),
            "avg_realized_wait_gain": float(work["realized_wait_gain"].mean()),
            "avg_policy_realized_saving": float(work["policy_realized_saving"].mean()),
        }


class FuelDecisionModelA:
    def __init__(
        self,
        fuel_col: str,
        horizon_days: int = 7,
        train_end_date: Optional[str] = None,
        waiting_cost: float = 0.003,
    ) -> None:
        self.fuel_col = fuel_col
        self.horizon_days = int(horizon_days)
        self.train_end_date = train_end_date
        self.waiting_cost = float(waiting_cost)
        self.feature_builder = FeatureBuilder(fuel_col=fuel_col, horizon_days=horizon_days)
        self.forecaster = HorizonMinForecaster(horizon_days=horizon_days)
        self.policy = WaitBuyDecisionPolicy(waiting_cost=waiting_cost)
        self.feature_columns_: list[str] = []
        self.training_frame_: Optional[pd.DataFrame] = None
        self.backtest_: Optional[BacktestResult] = None

    def build_training_frame(self, panel_df: pd.DataFrame) -> pd.DataFrame:
        frame = self.feature_builder.build(panel_df)
        feature_columns = [
            "station_id",
            "regime",
            "price",
            "price_ffill",
            "market_min",
            "market_mean",
            "market_median",
            "market_std",
            "delta_1",
            "delta_3",
            "delta_7",
            "rolling_std_14",
            "rolling_mean_14",
            "days_since_reset",
            "recent_resets_90",
            "lag_1",
            "lag_2",
            "lag_3",
            "lag_7",
            "lag_14",
            "lag_30",
            "change_1",
            "change_3",
            "change_7",
            "change_14",
            "roll_mean_7",
            "roll_std_7",
            "roll_mean_30",
            "roll_std_30",
            "price_position_365",
            "spread_to_market_median",
            "spread_to_market_min",
            "price_rank_pct",
            "n_cheaper_stations",
            "day_of_week",
            "month",
            "dow_sin",
            "dow_cos",
            "month_sin",
            "month_cos",
            "is_observed_price",
        ]
        self.feature_columns_ = [c for c in feature_columns if c in frame.columns]
        self.training_frame_ = frame
        return frame

    def fit(self, panel_df: pd.DataFrame) -> "FuelDecisionModelA":
        frame = self.build_training_frame(panel_df)
        target_column = f"target_min_{self.horizon_days}d"
        eligible = frame[(frame["is_observed_price"] == 1) & (frame["target_available"] == 1)].copy()
        eligible = eligible.dropna(subset=["price"])
        if eligible.empty:
            raise ValueError("No hay filas elegibles para entrenar el modelo.")

        if self.train_end_date:
            cutoff = pd.Timestamp(self.train_end_date)
        else:
            unique_dates = sorted(eligible["date"].dropna().unique())
            cutoff = pd.Timestamp(unique_dates[int(len(unique_dates) * 0.8)])

        train_df = eligible[eligible["date"] <= cutoff].copy()
        test_df = eligible[eligible["date"] > cutoff].copy()
        if train_df.empty or test_df.empty:
            raise ValueError("El split temporal quedó vacío. Ajusta train_end_date u horizonte.")

        self.forecaster.fit(train_df, feature_columns=self.feature_columns_, target_column=target_column)
        pred_test = self.forecaster.predict(test_df)
        metrics = self._evaluate_predictions(pred_test, target_column=target_column)
        policy_summary = self.policy.summarize(pred_test[[
            "price", "regime", target_column, *[f"pred_q{int(q*100):02d}" for q in self.forecaster.quantiles]
        ]].rename(columns={target_column: target_column}))
        self.backtest_ = BacktestResult(
            horizon_days=self.horizon_days,
            rows_train=len(train_df),
            rows_test=len(test_df),
            metrics=metrics,
            policy_summary=policy_summary,
        )
        return self

    def predict_latest(self, panel_df: pd.DataFrame) -> pd.DataFrame:
        frame = self.build_training_frame(panel_df)
        latest_date = frame["date"].max()
        latest = frame[(frame["date"] == latest_date) & (frame["is_observed_price"] == 1)].copy()
        preds = self.forecaster.predict(latest)
        result = self.policy.apply(preds[[
            "date", "station_id", "price", "regime", *[f"pred_q{int(q*100):02d}" for q in self.forecaster.quantiles], f"target_min_{self.horizon_days}d"
        ]].copy())
        return result.sort_values(["decision", "price", "station_id"])

    def save(self, path: str | Path) -> str:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "fuel_col": self.fuel_col,
            "horizon_days": self.horizon_days,
            "train_end_date": self.train_end_date,
            "waiting_cost": self.waiting_cost,
            "feature_columns_": self.feature_columns_,
            "forecaster": self.forecaster,
            "backtest": self.backtest_.to_dict() if self.backtest_ else None,
        }
        joblib.dump(payload, path)
        return str(path)

    @staticmethod
    def load(path: str | Path) -> "FuelDecisionModelA":
        payload = joblib.load(path)
        model = FuelDecisionModelA(
            fuel_col=payload["fuel_col"],
            horizon_days=payload["horizon_days"],
            train_end_date=payload["train_end_date"],
            waiting_cost=payload["waiting_cost"],
        )
        model.feature_columns_ = payload["feature_columns_"]
        model.forecaster = payload["forecaster"]
        if payload.get("backtest"):
            bt = payload["backtest"]
            model.backtest_ = BacktestResult(
                horizon_days=bt["horizon_days"],
                rows_train=bt["rows_train"],
                rows_test=bt["rows_test"],
                metrics=bt["metrics"],
                policy_summary=bt["policy_summary"],
            )
        return model

    def backtest_summary_json(self) -> str:
        return json.dumps(self.backtest_.to_dict() if self.backtest_ else {}, indent=2, ensure_ascii=False)

    @staticmethod
    def _evaluate_predictions(df: pd.DataFrame, target_column: str) -> dict:
        y_true = df[target_column]
        metrics = {
            "mae_q50": float(mean_absolute_error(y_true, df["pred_q50"])),
        }
        for q in DEFAULT_QUANTILES:
            col = f"pred_q{int(q*100):02d}"
            metrics[f"pinball_q{int(q*100):02d}"] = float(mean_pinball_loss(y_true, df[col], alpha=q))
        return metrics


def build_default_panel(
    fuel_col: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db=None,
) -> pd.DataFrame:
    builder = FuelDatasetBuilder(db=db)
    return builder.build_station_panel(
        start_date=start_date,
        end_date=end_date,
        include_metadata=True,
        drop_all_null_prices=False,
        include_full_calendar=True,
        include_fetch_status=True,
    )
