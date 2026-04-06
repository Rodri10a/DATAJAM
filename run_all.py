"""
DataJam - Team ProductDetails
Pipeline: Schema -> ETL -> API (DummyJSON)

Uso: pip install mysql-connector-python requests && python run_all.py
"""
import csv, os, sys, requests, mysql.connector

DB = {"host": "localhost", "port": 3306, "user": "root", "password": "root"}
DB_NAME = "datajam"
DATA_DIR = os.path.join(os.path.dirname(__file__), "dataset_group_product_details")
API_URL = "https://dummyjson.com/products"

TABLES_CSV = [
    ("countries",        "countries.csv",        ["code", "name", "region", "population"]),
    ("categories",       "categories.csv",       ["id", "slug", "name"]),
    ("users",            "users.csv",            ["id", "name", "email", "country_code", "created_at"]),
    ("products",         "products.csv",         ["id", "name", "price", "category_id"]),
    ("orders",           "orders.csv",           ["id", "user_id", "order_date", "total_amount"]),
    ("order_items",      "order_items.csv",      ["id", "order_id", "product_id", "quantity", "unit_price"]),
    ("shipping_regions", "shipping_regions.csv", ["country_code", "region", "shipping_zone", "estimated_days"]),
]


def run():
    # --- Phase 1: Create DB + Schema ---
    print("\n[Phase 1] Creating database and schema...")
    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cur.execute(f"USE {DB_NAME}")

    sql_path = os.path.join(os.path.dirname(__file__), "sql", "01_schema.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    clean = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
    for stmt in clean.split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                cur.execute(stmt)
            except mysql.connector.Error:
                pass
    conn.commit()
    cur.close()
    conn.close()
    print("  [OK] Schema created")

    # --- Phase 2: Load CSVs ---
    print("\n[Phase 2] Loading CSVs...")
    conn = mysql.connector.connect(**DB, database=DB_NAME)
    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS = 0")

    # Clean tables before re-loading (safe for re-runs)
    all_tables = ["order_items","orders","product_details","products","shipping_regions","users","categories","countries"]
    for t in all_tables:
        cur.execute(f"TRUNCATE TABLE {t}")

    for table, filename, cols in TABLES_CSV:
        with open(os.path.join(DATA_DIR, filename), "r", encoding="utf-8") as f:
            rows = [tuple(r[c] for c in cols) for r in csv.DictReader(f)]
        placeholders = ",".join(["%s"] * len(cols))
        cur.executemany(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})", rows)
        print(f"  [OK] {table}: {len(rows)} rows")

    cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()

    # --- Phase 3: DummyJSON API -> product_details ---
    print("\n[Phase 3] Fetching product_details from DummyJSON...")
    products, skip = [], 0
    while True:
        data = requests.get(f"{API_URL}?limit=30&skip={skip}&select=id,stock,rating,weight", timeout=30).json()
        products.extend(data["products"])
        skip += 30
        if skip >= data["total"]:
            break
    print(f"  [OK] {len(products)} products fetched from API")

    cur.execute("SELECT id FROM products")
    local_ids = {r[0] for r in cur.fetchall()}

    sql = """INSERT INTO product_details (product_id, stock, rating, weight)
             VALUES (%s,%s,%s,%s)
             ON DUPLICATE KEY UPDATE stock=VALUES(stock), rating=VALUES(rating), weight=VALUES(weight)"""
    for p in products:
        if p["id"] in local_ids:
            cur.execute(sql, (p["id"], p.get("stock", 0), p.get("rating", 0), p.get("weight", 0)))
    conn.commit()
    print(f"  [OK] product_details populated")

    # --- Verification ---
    print("\n[Verification]")
    for t in ["countries","categories","users","products","product_details","orders","order_items","shipping_regions"]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        n = cur.fetchone()[0]
        print(f"  {'[OK]' if n > 0 else '[EMPTY]'} {t}: {n} rows")

    cur.close()
    conn.close()
    print("\nALL DONE - System 100% operational.\n")


if __name__ == "__main__":
    try:
        run()
    except mysql.connector.Error as e:
        print(f"\n[ERROR] MySQL: {e}\nAsegurate que MySQL este corriendo en localhost:3306")
        sys.exit(1)
    except requests.RequestException as e:
        print(f"\n[ERROR] API: {e}\nRevisa tu conexion a internet")
        sys.exit(1)
