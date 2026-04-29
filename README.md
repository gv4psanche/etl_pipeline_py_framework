# ETL Pipeline Python Framework

## ETL Solution — What's Included

### Project Structure 

etl_solution/<br>
├── etl_pipeline.py          # Main ETL script (Extract → Transform → Load)<br>
├── scheduler.py             # Automated scheduling (daily/hourly/weekly)<br>
├── requirements.txt         # Python dependencies<br>
├── config/<br>
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── etl_config.yaml      # All source/output settings<br>
├── data/<br>
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;├── raw/                 # Input: sales.csv, customers.csv, products.csv<br>
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── processed/           # Output: 6 CSVs + combined Excel (6 sheets)<br>
└── logs/<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── etl_pipeline.log     # Full run logs<br>

---

## 🔁 <b>The Pipeline (etl_pipeline.py)</b>
### Extract — Three pluggable sources, switch via --source flag:

- Source local — reads CSVs from data/raw/ (default / sample mode)
- Source azure — downloads blobs from Azure Blob Storage using azure-storage-blob
- Source databricks — queries Delta tables via databricks-sdk

### Transform — Runs automatically on all sources:

- Parses date columns, drops inactive products
- Builds a Fact_Orders table joining all 3 sources with derived columns: revenue, cost, gross_profit, profit_margin_%, high_discount, date parts (year/month/quarter)
- Builds Dim_Customers and Dim_Products with enriched fields
- Generates 3 summary aggregates: by Region/Quarter, by Product, by Sales Rep

### Load — Outputs everything BI-ready:

- 6 individual CSVs (for Tableau drag-and-drop)
- 1 multi-sheet Excel file with auto-fit columns (for Power BI Get Data → Excel)

---
## ▶️ How to Run
- bash# Install dependencies
- pip install -r requirements.txt

###  Run with local sample data
- python etl_pipeline.py --source local

###  Run with Azure Blob (fill in credentials in config/etl_config.yaml first)
- python etl_pipeline.py --source azure

###  Run with Databricks
- python etl_pipeline.py --source databricks

###  Schedule automatically (runs daily at 6am)
- python scheduler.py --source azure --interval daily --time 06:00
