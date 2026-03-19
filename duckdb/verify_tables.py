"""Verify Bronze tables were created correctly"""
import duckdb

DB_PATH = "D:/NUS_MQF/QF5214/5214_Project_SPX_Index_Raw_Data/duckdb/spx_analytics.duckdb"

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
