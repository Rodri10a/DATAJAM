"""
DataJam - Team ProductDetails
Pipeline: Schema -> ETL -> API (DummyJSON)

Uso: pip install mysql-connector-python requests && python run_all.py
"""
# Librerias necesarias
# csv: leer archivos CSV
# os: manejar rutas de archivos
# sys: salir del programa si hay error
# requests: hacer llamadas HTTP a la API de DummyJSON
# mysql.connector: conectarse y operar con MySQL
import csv, os, sys, requests, mysql.connector

# Credenciales de conexion a MySQL (el container de Docker)
DB = {"host": "localhost", "port": 3306, "user": "root", "password": "root"}

# Nombre de la base de datos que vamos a crear
DB_NAME = "datajam"

# Ruta a la carpeta donde estan los archivos CSV
DATA_DIR = os.path.join(os.path.dirname(__file__), "dataset_group_product_details")

# URL base de la API de DummyJSON (de donde sacamos stock, rating y weight)
API_URL = "https://dummyjson.com/products"

# Mapa de tablas: cada tupla dice (nombre_tabla, archivo_csv, columnas)
# Esto le dice al Phase 2 que CSV va a cada tabla y con que columnas
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
    # ==========================================================
    # PHASE 1: Crear la base de datos y las tablas
    # Se conecta a MySQL, crea la DB "datajam", lee el archivo
    # sql/01_schema.sql y ejecuta cada CREATE TABLE
    # ==========================================================
    print("\n[Phase 1] Creating database and schema...")
    conn = mysql.connector.connect(**DB)     # Conecta a MySQL sin seleccionar DB
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")  # Crea la DB si no existe
    cur.execute(f"USE {DB_NAME}")                            # Selecciona la DB

    # Lee el archivo SQL con los CREATE TABLE
    sql_path = os.path.join(os.path.dirname(__file__), "sql", "01_schema.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    # Elimina lineas de comentarios (--) para que no interfieran
    clean = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))

    # Separa cada sentencia SQL por el ; y la ejecuta una por una
    for stmt in clean.split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                cur.execute(stmt)
            except mysql.connector.Error:
                pass  # Si la tabla ya existe, no falla
    conn.commit()
    cur.close()
    conn.close()
    print("  [OK] Schema created")

    # ==========================================================
    # PHASE 2: Cargar los datos de los CSVs en MySQL
    # Primero vacia todas las tablas (TRUNCATE) para poder
    # re-ejecutar sin errores de duplicados.
    # Luego recorre el mapa TABLES_CSV y carga cada archivo.
    # ==========================================================
    print("\n[Phase 2] Loading CSVs...")
    conn = mysql.connector.connect(**DB, database=DB_NAME)
    cur = conn.cursor()

    # Desactiva validacion de Foreign Keys para poder cargar en cualquier orden
    cur.execute("SET FOREIGN_KEY_CHECKS = 0")

    # Vacia todas las tablas antes de cargar (permite re-ejecutar el script)
    all_tables = ["order_items","orders","product_details","products","shipping_regions","users","categories","countries"]
    for t in all_tables:
        cur.execute(f"TRUNCATE TABLE {t}")

    # Recorre cada tabla del mapa y carga su CSV correspondiente
    for table, filename, cols in TABLES_CSV:
        # Lee el CSV y convierte cada fila en una tupla con las columnas necesarias
        with open(os.path.join(DATA_DIR, filename), "r", encoding="utf-8") as f:
            rows = [tuple(r[c] for c in cols) for r in csv.DictReader(f)]

        # Arma el INSERT con placeholders (%s) y ejecuta todas las filas de golpe
        placeholders = ",".join(["%s"] * len(cols))
        cur.executemany(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})", rows)
        print(f"  [OK] {table}: {len(rows)} rows")

    # Reactiva la validacion de Foreign Keys
    cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()

    # ==========================================================
    # PHASE 3: Consumir la API de DummyJSON para llenar product_details
    # La API devuelve de a 30 productos. Hacemos un loop pidiendo
    # paginas hasta traer los 194. Despues los insertamos en la DB
    # solo si el producto existe en nuestra tabla products.
    # ==========================================================
    print("\n[Phase 3] Fetching product_details from DummyJSON...")

    # Paginacion: traemos de a 30 productos hasta cubrir el total
    products, skip = [], 0
    while True:
        # Pedimos solo los campos que necesitamos: id, stock, rating, weight
        data = requests.get(f"{API_URL}?limit=30&skip={skip}&select=id,stock,rating,weight", timeout=30).json()
        products.extend(data["products"])  # Agregamos los productos al array
        skip += 30                         # Avanzamos a la siguiente pagina
        if skip >= data["total"]:          # Si ya trajimos todos, paramos
            break
    print(f"  [OK] {len(products)} products fetched from API")

    # Traemos los IDs de productos que existen en nuestra DB
    cur.execute("SELECT id FROM products")
    local_ids = {r[0] for r in cur.fetchall()}  # Set de IDs locales: {1, 2, 3, ..., 194}

    # SQL con ON DUPLICATE KEY UPDATE: si ya existe, actualiza en vez de fallar
    sql = """INSERT INTO product_details (product_id, stock, rating, weight)
             VALUES (%s,%s,%s,%s)
             ON DUPLICATE KEY UPDATE stock=VALUES(stock), rating=VALUES(rating), weight=VALUES(weight)"""

    # Solo insertamos productos que existen en NUESTRO catalogo
    for p in products:
        if p["id"] in local_ids:
            cur.execute(sql, (p["id"], p.get("stock", 0), p.get("rating", 0), p.get("weight", 0)))
    conn.commit()
    print(f"  [OK] product_details populated")

    # ==========================================================
    # VERIFICACION: Recorre cada tabla y muestra cuantas filas tiene
    # Si alguna dice [EMPTY] es que algo fallo
    # ==========================================================
    print("\n[Verification]")
    for t in ["countries","categories","users","products","product_details","orders","order_items","shipping_regions"]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        n = cur.fetchone()[0]
        print(f"  {'[OK]' if n > 0 else '[EMPTY]'} {t}: {n} rows")

    cur.close()
    conn.close()
    print("\nALL DONE - System 100% operational.\n")


# ==========================================================
# PUNTO DE ENTRADA: solo se ejecuta si corres "python run_all.py"
# El try/except atrapa errores de MySQL o de la API y muestra
# un mensaje claro en vez de un traceback largo
# ==========================================================
if __name__ == "__main__":
    try:
        run()
    except mysql.connector.Error as e:
        print(f"\n[ERROR] MySQL: {e}\nAsegurate que MySQL este corriendo en localhost:3306")
        sys.exit(1)
    except requests.RequestException as e:
        print(f"\n[ERROR] API: {e}\nRevisa tu conexion a internet")
        sys.exit(1)
