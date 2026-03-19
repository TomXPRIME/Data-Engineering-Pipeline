"""Initialize DuckDB Bronze Layer tables"""
import duckdb

DB_PATH = "D:/NUS_MQF/QF5214/5214_Project_SPX_Index_Raw_Data/duckdb/spx_analytics.duckdb"

con = duckdb.connect(DB_PATH)

# Execute the SQL script
with open("D:/NUS_MQF/QF5214/5214_Project_SPX_Index_Raw_Data/duckdb/create_bronze_tables.sql", "r") as f:
    sql = f.read()

con.execute(sql)
con.close()

print(f"Bronze tables created in {DB_PATH}")
