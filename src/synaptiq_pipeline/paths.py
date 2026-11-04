"""Immutable filesystem layout for Bronze Delta, Intermediate Parquet, Silver Delta, and Gold Delta."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class LakehousePaths:
    """Resolved paths for every Medallion layer and intermediate Parquet enrichment step."""

    warehouse_root: Path
    output_root: Path

    @classmethod
    def from_roots(
        cls,
        warehouse_root: Path | str = Path("warehouse"),
        output_root: Path | str = Path("output"),
    ) -> LakehousePaths:
        """Construct resolved LakehousePaths from warehouse and output root directories."""
        return cls(
            warehouse_root=Path(warehouse_root).resolve(),
            output_root=Path(output_root).resolve(),
        )

    @property
    def bronze_orders(self) -> Path:
        """Return the Bronze Delta directory for raw daily order drops."""
        return self.warehouse_root / "bronze" / "orders"

    @property
    def bronze_products(self) -> Path:
        """Return the Bronze Delta directory for raw product reference files."""
        return self.warehouse_root / "bronze" / "products"

    @property
    def step_01_products_cleaned_parquet(self) -> Path:
        """Return the intermediate Parquet path for standardized product rows."""
        return self.warehouse_root / "intermediate" / "01_products_cleaned.parquet"

    @property
    def step_01_orders_parsed_parquet(self) -> Path:
        """Return the intermediate Parquet path for typed and UTC-parsed order rows."""
        return self.warehouse_root / "intermediate" / "01_orders_parsed_fields.parquet"

    @property
    def step_02_orders_traps_parquet(self) -> Path:
        """Return the intermediate Parquet path for row-level trap-annotated orders."""
        return self.warehouse_root / "intermediate" / "02_orders_row_traps.parquet"

    @property
    def step_03_orders_reconciled_parquet(self) -> Path:
        """Return the intermediate Parquet path for cross-drop reconciled winning orders."""
        return self.warehouse_root / "intermediate" / "03_orders_reconciled.parquet"

    @property
    def step_03_orders_superseded_parquet(self) -> Path:
        """Return the intermediate Parquet path for superseded duplicate/prior order versions."""
        return self.warehouse_root / "intermediate" / "03_orders_cdc_superseded.parquet"

    @property
    def silver_products(self) -> Path:
        """Return the Silver Delta directory for canonical product reference records."""
        return self.warehouse_root / "silver" / "products"

    @property
    def silver_orders(self) -> Path:
        """Return the Silver Delta directory for canonical MERGE-reconciled orders."""
        return self.warehouse_root / "silver" / "orders"

    @property
    def silver_orders_superseded(self) -> Path:
        """Return the Silver Delta directory archiving superseded cross-drop order rows."""
        return self.warehouse_root / "silver" / "orders_cdc_superseded"

    @property
    def step_04_joined_order_facts_parquet(self) -> Path:
        """Return the intermediate Parquet path for catalog-joined order fact rows."""
        return self.warehouse_root / "intermediate" / "04_joined_order_facts.parquet"

    @property
    def gold_daily_sales(self) -> Path:
        """Return the Gold Delta directory for daily sales per category and region."""
        return self.warehouse_root / "gold" / "daily_sales"

    @property
    def gold_trap_forensics(self) -> Path:
        """Return the Gold Delta directory for aggregated forensic trap audit metrics."""
        return self.warehouse_root / "gold" / "trap_forensics"

    @property
    def daily_sales_csv(self) -> Path:
        """Return the single-file CSV export path for the Gold daily_sales table."""
        return self.output_root / "daily_sales.csv"

    @property
    def trap_forensics_csv(self) -> Path:
        """Return the single-file CSV export path for the forensic trap audit report."""
        return self.output_root / "trap_forensics.csv"
