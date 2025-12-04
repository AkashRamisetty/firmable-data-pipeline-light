import psycopg2

def main():
    conn = psycopg2.connect(
        dbname="firmable_companies",
        user="firmable",
        password="firmable_password",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    print("✅ Connected to:", cur.fetchone()[0])
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
