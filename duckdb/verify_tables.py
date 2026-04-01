"""Verify Bronze tables were created correctly"""
from pathlib import Path
import duckdb

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
DB_PATH = PROJECT_ROOT / "duckdb" / "spx_analytics.duckdb"

con = duckdb.connect(DB_PATH)

# List all tables
print("=== Tables in database ===")
tables = con.execute("SHOW TABLES").fetchall()
for t in tables:
    print(f"  {t[0]}")

# Show table schemas
print("\n=== Table schemas ===")
for t in tables:
    name = t[0]
    print(f"\n{name}:")
    cols = con.execute(f"DESCRIBE {name}").fetchall()
    for c in cols:
        print(f"  {c[0]}: {c[1]}")

con.close()
