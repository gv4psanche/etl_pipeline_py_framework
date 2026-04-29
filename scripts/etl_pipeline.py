"""
============================================================
  etl_pipeline.py  –  Automated ETL Solution
  Sources : Azure Blob Storage  |  Databricks
  Targets : Power BI / Tableau (CSV + Excel)
============================================================
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

# ── Optional cloud-SDK imports (gracefully skip if not installed) ──────────
try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from databricks import sdk as databricks_sdk          # databricks-sdk
    from databricks.sdk import WorkspaceClient
    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

# ══════════════════════════════════════════════════════════════════════════════
#  SETUP
# ══════════════════════════════════════════════════════════════════════════════

def load_config(config_path: str = "config/etl_config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def setup_logging(cfg: dict) -> logging.Logger:
    log_dir  = Path(cfg["logging"]["log_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / cfg["logging"]["log_file"]

    logging.basicConfig(
        level   = getattr(logging, cfg["logging"]["level"]),
        format  = "%(asctime)s  [%(levelname)-8s]  %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("ETL")


# ══════════════════════════════════════════════════════════════════════════════
#  EXTRACT
# ══════════════════════════════════════════════════════════════════════════════

class Extractor:
    def __init__(self, cfg: dict, logger: logging.Logger):
        self.cfg    = cfg
        self.logger = logger

    # ── Azure Blob ────────────────────────────────────────────────────────────
    def extract_from_azure_blob(self) -> dict[str, pd.DataFrame]:
        if not AZURE_AVAILABLE:
            self.logger.error("azure-storage-blob not installed. Run: pip install azure-storage-blob")
            sys.exit(1)

        blob_cfg    = self.cfg["azure_blob"]
        staging_dir = Path(blob_cfg["local_staging_dir"])
        staging_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info("Connecting to Azure Blob Storage …")
        client    = BlobServiceClient.from_connection_string(blob_cfg["connection_string"])
        container = client.get_container_client(blob_cfg["container_name"])

        dataframes: dict[str, pd.DataFrame] = {}
        for blob_name in blob_cfg["blobs"]:
            local_path = staging_dir / blob_name
            self.logger.info(f"  Downloading blob: {blob_name}")
            blob_data = container.download_blob(blob_name).readall()
            local_path.write_bytes(blob_data)

            key = Path(blob_name).stem          # e.g. "sales"
            dataframes[key] = pd.read_csv(local_path)
            self.logger.info(f"  Loaded {key}: {dataframes[key].shape[0]} rows")

        return dataframes

    # ── Databricks ────────────────────────────────────────────────────────────
    def extract_from_databricks(self) -> dict[str, pd.DataFrame]:
        if not DATABRICKS_AVAILABLE:
            self.logger.error("databricks-sdk not installed. Run: pip install databricks-sdk")
            sys.exit(1)

        db_cfg  = self.cfg["databricks"]
        staging = Path(db_cfg["local_staging_dir"])
        staging.mkdir(parents=True, exist_ok=True)

        self.logger.info("Connecting to Databricks …")
        w = WorkspaceClient(host=db_cfg["host"], token=db_cfg["token"])

        dataframes: dict[str, pd.DataFrame] = {}
        for full_table in db_cfg["tables"]:
            key = full_table.split(".")[-1]     # last part = table name
            self.logger.info(f"  Reading Databricks table: {full_table}")
            result = w.statement_execution.execute_statement(
                warehouse_id = db_cfg.get("warehouse_id", ""),
                statement    = f"SELECT * FROM {full_table}",
            )
            rows    = [r for r in result.result.data_array]
            columns = [c.name for c in result.manifest.schema.columns]
            df = pd.DataFrame(rows, columns=columns)

            local_path = staging / f"{key}.csv"
            df.to_csv(local_path, index=False)
            dataframes[key] = df
            self.logger.info(f"  Loaded {key}: {df.shape[0]} rows")

        return dataframes

    # ── Local (sample / fallback) ─────────────────────────────────────────────
    def extract_from_local(self, directory: str = "data/raw") -> dict[str, pd.DataFrame]:
        self.logger.info(f"Extracting CSVs from local directory: {directory}")
        dataframes: dict[str, pd.DataFrame] = {}
        for csv_file in Path(directory).glob("*.csv"):
            key = csv_file.stem
            dataframes[key] = pd.read_csv(csv_file)
            self.logger.info(f"  Loaded {key}: {dataframes[key].shape[0]} rows × {dataframes[key].shape[1]} cols")
        return dataframes


# ══════════════════════════════════════════════════════════════════════════════
#  TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════

class Transformer:
    def __init__(self, cfg: dict, logger: logging.Logger):
        self.cfg    = cfg["transformations"]
        self.logger = logger

    def run(self, raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        self.logger.info("─── TRANSFORM phase ───")
        sales     = raw.get("sales",     pd.DataFrame())
        customers = raw.get("customers", pd.DataFrame())
        products  = raw.get("products",  pd.DataFrame())

        # ── 1. Parse dates ────────────────────────────────────────────────────
        for df in [sales, customers, products]:
            for col in self.cfg["date_columns"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

        # ── 2. Filter inactive products ───────────────────────────────────────
        if self.cfg["drop_inactive_products"] and "is_active" in products.columns:
            before = len(products)
            products = products[products["is_active"].astype(str).str.upper() == "TRUE"].copy()
            self.logger.info(f"  Dropped {before - len(products)} inactive products")

        # ── 3. Build Fact: Orders ─────────────────────────────────────────────
        fact = sales.merge(customers, on="customer_id", how="left") \
                    .merge(products,  on="product_id",  how="left")

        # Derived financial columns
        fact["revenue"]         = fact["quantity"] * fact["unit_price"] * (1 - fact["discount"])
        fact["cost"]            = fact["quantity"] * fact["cost_price"]
        fact["gross_profit"]    = fact["revenue"] - fact["cost"]
        fact["profit_margin_%"] = (fact["gross_profit"] / fact["revenue"].replace(0, pd.NA) * 100).round(2)
        fact["high_discount"]   = fact["discount"] > self.cfg["discount_threshold"]

        # Date parts (useful for BI slicers)
        fact["order_year"]    = fact["order_date"].dt.year
        fact["order_month"]   = fact["order_date"].dt.month
        fact["order_month_name"] = fact["order_date"].dt.strftime("%B")
        fact["order_quarter"] = fact["order_date"].dt.quarter

        # Revenue rounded
        fact["revenue"]      = fact["revenue"].round(2)
        fact["gross_profit"] = fact["gross_profit"].round(2)
        fact["cost"]         = fact["cost"].round(2)

        self.logger.info(f"  Fact table built: {len(fact)} rows, {len(fact.columns)} columns")

        # ── 4. Dimension: Customers ───────────────────────────────────────────
        dim_customers = customers.copy()
        dim_customers["customer_tenure_days"] = (
            pd.Timestamp.today() - dim_customers["join_date"]
        ).dt.days

        # ── 5. Dimension: Products ────────────────────────────────────────────
        dim_products = products.copy()
        dim_products["margin_%"] = (
            (dim_products["list_price"] - dim_products["cost_price"])
            / dim_products["list_price"] * 100
        ).round(2)

        # ── 6. Summary tables ─────────────────────────────────────────────────
        summary_region = (
            fact.groupby(["region", "order_year", "order_quarter"])
            .agg(
                total_orders   = ("order_id",    "count"),
                total_revenue  = ("revenue",     "sum"),
                total_profit   = ("gross_profit","sum"),
                avg_discount   = ("discount",    "mean"),
            )
            .reset_index()
            .round(2)
        )

        summary_product = (
            fact.groupby(["product_name", "category"])
            .agg(
                units_sold     = ("quantity",    "sum"),
                total_revenue  = ("revenue",     "sum"),
                total_profit   = ("gross_profit","sum"),
                avg_margin     = ("profit_margin_%","mean"),
            )
            .reset_index()
            .round(2)
        )

        summary_rep = (
            fact.groupby(["sales_rep", "region"])
            .agg(
                total_orders   = ("order_id",    "count"),
                total_revenue  = ("revenue",     "sum"),
                total_profit   = ("gross_profit","sum"),
            )
            .reset_index()
            .round(2)
        )

        transformed = {
            "fact_orders"        : fact,
            "dim_customers"      : dim_customers,
            "dim_products"       : dim_products,
            "summary_by_region"  : summary_region,
            "summary_by_product" : summary_product,
            "summary_by_rep"     : summary_rep,
        }

        for name, df in transformed.items():
            self.logger.info(f"  Output  '{name}': {df.shape[0]} rows × {df.shape[1]} cols")

        return transformed


# ══════════════════════════════════════════════════════════════════════════════
#  LOAD
# ══════════════════════════════════════════════════════════════════════════════

class Loader:
    def __init__(self, cfg: dict, logger: logging.Logger):
        self.cfg    = cfg["output"]
        self.logger = logger

    def run(self, data: dict[str, pd.DataFrame]) -> None:
        self.logger.info("─── LOAD phase ───")
        out_dir = Path(self.cfg["directory"])
        out_dir.mkdir(parents=True, exist_ok=True)

        # ── Individual CSVs ───────────────────────────────────────────────────
        file_map = self.cfg["files"]
        for key, filename in file_map.items():
            if key in data:
                path = out_dir / filename
                data[key].to_csv(path, index=False)
                self.logger.info(f"  Saved CSV  → {path}")

        # ── Combined Excel (multi-sheet) for Power BI / Tableau ───────────────
        excel_path = out_dir / self.cfg["excel_combined"]
        sheet_map = {
            "fact_orders"        : "Fact_Orders",
            "dim_customers"      : "Dim_Customers",
            "dim_products"       : "Dim_Products",
            "summary_by_region"  : "Summary_Region",
            "summary_by_product" : "Summary_Product",
            "summary_by_rep"     : "Summary_SalesRep",
        }

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            for key, sheet_name in sheet_map.items():
                if key in data:
                    df = data[key]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

                    # Auto-fit column widths
                    ws = writer.sheets[sheet_name]
                    for col_cells in ws.columns:
                        max_len = max(
                            len(str(cell.value)) if cell.value else 0
                            for cell in col_cells
                        )
                        ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)

        self.logger.info(f"  Saved Excel → {excel_path}  ({len(sheet_map)} sheets)")
        self.logger.info("  ✅ Load phase complete.")


# ══════════════════════════════════════════════════════════════════════════════
#  ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

class ETLPipeline:
    """Orchestrates Extract → Transform → Load."""

    def __init__(self, config_path: str = "config/etl_config.yaml"):
        self.cfg    = load_config(config_path)
        self.logger = setup_logging(self.cfg)

    def run(self, source: str = "local") -> None:
        start = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info("  ETL Pipeline  –  START")
        self.logger.info(f"  Source: {source.upper()}   |   {start.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 60)

        extractor   = Extractor(self.cfg, self.logger)
        transformer = Transformer(self.cfg, self.logger)
        loader      = Loader(self.cfg, self.logger)

        # ── EXTRACT ───────────────────────────────────────────────────────────
        self.logger.info("─── EXTRACT phase ───")
        if source == "azure":
            raw = extractor.extract_from_azure_blob()
        elif source == "databricks":
            raw = extractor.extract_from_databricks()
        else:
            raw = extractor.extract_from_local()

        # ── TRANSFORM ─────────────────────────────────────────────────────────
        transformed = transformer.run(raw)

        # ── LOAD ──────────────────────────────────────────────────────────────
        loader.run(transformed)

        elapsed = (datetime.now() - start).total_seconds()
        self.logger.info("=" * 60)
        self.logger.info(f"  ETL Pipeline  –  COMPLETE  ({elapsed:.2f}s)")
        self.logger.info("=" * 60)


# ══════════════════════════════════════════════════════════════════════════════
#  CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automated ETL Pipeline")
    parser.add_argument(
        "--source",
        choices=["local", "azure", "databricks"],
        default="local",
        help="Data source: local (default), azure, or databricks",
    )
    parser.add_argument(
        "--config",
        default="config/etl_config.yaml",
        help="Path to YAML config file",
    )
    args = parser.parse_args()

    pipeline = ETLPipeline(config_path=args.config)
    pipeline.run(source=args.source)
