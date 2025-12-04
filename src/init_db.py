import psycopg2
from pathlib import Path

def main():
    ddl_path = Path(__file__).resolve().parent.parent / "sql" / "ddl_tables.sql"
    sql = ddl_path.read_text()

    conn = psycopg2.connect(
        dbname="firmable_companies",
        user="firmable",
        password="firmable_password",
        host="localhost",
        port=5432,
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()
    print(f"✅ Applied DDL from {ddl_path}")

if __name__ == "__main__":
    main()
