from __future__ import annotations

"""Orquestación ligera de recolección + exportación de datasets."""

from dataclasses import dataclass
from typing import Optional

from src.data_collection import FuelDataCollector, CollectionSummary
from src.dataset_builder import FuelDatasetBuilder, default_dataset_path


@dataclass(slots=True)
class CollectionPipelineResult:
    collection: CollectionSummary
    dataset_path: str
    dataset_type: str

    def to_dict(self) -> dict:
        return {
            "collection": self.collection.to_dict(),
            "dataset_path": self.dataset_path,
            "dataset_type": self.dataset_type,
        }


def collect_and_export_dataset(
    start_date: str,
    end_date: Optional[str] = None,
    dataset_type: str = "panel",
    output_path: Optional[str] = None,
    force: bool = False,
    allow_empty_dates: bool = True,
    include_full_calendar: bool = False,
    stop_on_error: bool = False,
) -> CollectionPipelineResult:
    collector = FuelDataCollector()
    summary = collector.collect_range(
        start_date=start_date,
        end_date=end_date,
        force=force,
        allow_empty_dates=allow_empty_dates,
        stop_on_error=stop_on_error,
    )

    builder = FuelDatasetBuilder(db=collector.db)
    final_output = output_path or default_dataset_path(dataset_type, suffix=".csv")
    path = builder.export_dataset(
        output_path=final_output,
        dataset_type=dataset_type,
        start_date=start_date,
        end_date=end_date,
        include_full_calendar=include_full_calendar,
    )
    return CollectionPipelineResult(
        collection=summary,
        dataset_path=path,
        dataset_type=dataset_type,
    )
