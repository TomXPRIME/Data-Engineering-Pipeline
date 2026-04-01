"""Initialize DuckDB Bronze Layer tables"""
from pathlib import Path
import duckdb

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
DB_PATH = PROJECT_ROOT / "duckdb" / "spx_analytics.duckdb"
SQL_PATH = SCRIPT_DIR / "create_bronze_tables.sql"

con = duckdb.connect(DB_PATH)

# Execute the SQL script
with open(SQL_PATH, "r") as f:
    sql = f.read()

con.execute(sql)
con.close()

print(f"Bronze tables created in {DB_PATH}")
