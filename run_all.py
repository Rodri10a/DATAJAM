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
MYSQL_CONNECTION = {"host": "localhost", "port": 3306, "user": "root", "password": "root"}

# Nombre de la base de datos que vamos a crear
DATABASE_NAME = "datajam"

# Ruta a la carpeta donde estan los archivos CSV
DATASET_DIRECTORY = os.path.join(os.path.dirname(__file__), "dataset_group_product_details")

# URL base de la API de DummyJSON (de donde sacamos stock, rating y weight)
DUMMYJSON_API_URL = "https://dummyjson.com/products"

# Mapa de tablas: cada tupla dice (nombre_tabla, archivo_csv, columnas)
# Esto le dice al Phase 2 que CSV va a cada tabla y con que columnas
TABLE_CSV_MAPPING = [
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
    connection = mysql.connector.connect(**MYSQL_CONNECTION)     # Conecta a MySQL sin seleccionar DB
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")  # Crea la DB si no existe
    cursor.execute(f"USE {DATABASE_NAME}")                            # Selecciona la DB

    # Lee el archivo SQL con los CREATE TABLE
    schema_path = os.path.join(os.path.dirname(__file__), "sql", "01_schema.sql")
    with open(schema_path, "r", encoding="utf-8") as schema_file:
        schema_sql = schema_file.read()

    # Elimina lineas de comentarios (--) para que no interfieran
    clean_sql = "\n".join(line for line in schema_sql.splitlines() if not line.strip().startswith("--"))

    # Separa cada sentencia SQL por el ; y la ejecuta una por una
    for statement in clean_sql.split(";"):
        statement = statement.strip()
        if statement:
            try:
                cursor.execute(statement)
            except mysql.connector.Error:
                pass  # Si la tabla ya existe, no falla
    connection.commit()
    cursor.close()
    connection.close()
    print("  [OK] Schema created")

    # ==========================================================
    # PHASE 2: Cargar los datos de los CSVs en MySQL
    # Primero vacia todas las tablas (TRUNCATE) para poder
    # re-ejecutar sin errores de duplicados.
    # Luego recorre el mapa TABLE_CSV_MAPPING y carga cada archivo.
    # ==========================================================
    print("\n[Phase 2] Loading CSVs...")
    connection = mysql.connector.connect(**MYSQL_CONNECTION, database=DATABASE_NAME)
    cursor = connection.cursor()

    # Desactiva validacion de Foreign Keys para poder cargar en cualquier orden
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

    # Vacia todas las tablas antes de cargar (permite re-ejecutar el script)
    tables_to_truncate = ["order_items","orders","product_details","products","shipping_regions","users","categories","countries"]
    for table_name in tables_to_truncate:
        cursor.execute(f"TRUNCATE TABLE {table_name}")

    # Recorre cada tabla del mapa y carga su CSV correspondiente
    for table_name, csv_filename, columns in TABLE_CSV_MAPPING:
        # Lee el CSV y convierte cada fila en una tupla con las columnas necesarias
        with open(os.path.join(DATASET_DIRECTORY, csv_filename), "r", encoding="utf-8") as csv_file:
            rows = [tuple(row[col] for col in columns) for row in csv.DictReader(csv_file)]

        # Arma el INSERT con placeholders (%s) y ejecuta todas las filas de golpe
        placeholders = ",".join(["%s"] * len(columns))
        cursor.executemany(f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})", rows)
        print(f"  [OK] {table_name}: {len(rows)} rows")

    # Reactiva la validacion de Foreign Keys
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    connection.commit()

    # ==========================================================
    # PHASE 3: Consumir la API de DummyJSON para llenar product_details
    # La API devuelve de a 30 productos. Hacemos un loop pidiendo
    # paginas hasta traer los 194. Despues los insertamos en la DB
    # solo si el producto existe en nuestra tabla products.
    # ==========================================================
    print("\n[Phase 3] Fetching product_details from DummyJSON...")

    # Paginacion: traemos de a 30 productos hasta cubrir el total
    api_products, skip_count = [], 0
    while True:
        # Pedimos solo los campos que necesitamos: id, stock, rating, weight
        response = requests.get(f"{DUMMYJSON_API_URL}?limit=30&skip={skip_count}&select=id,stock,rating,weight", timeout=30).json()
        api_products.extend(response["products"])  # Agregamos los productos al array
        skip_count += 30                            # Avanzamos a la siguiente pagina
        if skip_count >= response["total"]:         # Si ya trajimos todos, paramos
            break
    print(f"  [OK] {len(api_products)} products fetched from API")

    # Traemos los IDs de productos que existen en nuestra DB
    cursor.execute("SELECT id FROM products")
    local_product_ids = {row[0] for row in cursor.fetchall()}  # Set de IDs locales: {1, 2, 3, ..., 194}

    # SQL con ON DUPLICATE KEY UPDATE: si ya existe, actualiza en vez de fallar
    upsert_sql = """INSERT INTO product_details (product_id, stock, rating, weight)
             VALUES (%s,%s,%s,%s)
             ON DUPLICATE KEY UPDATE stock=VALUES(stock), rating=VALUES(rating), weight=VALUES(weight)"""

    # Solo insertamos productos que existen en NUESTRO catalogo
    for product in api_products:
        if product["id"] in local_product_ids:
            cursor.execute(upsert_sql, (product["id"], product.get("stock", 0), product.get("rating", 0), product.get("weight", 0)))
    connection.commit()
    print(f"  [OK] product_details populated")

    # ==========================================================
    # VERIFICACION: Recorre cada tabla y muestra cuantas filas tiene
    # Si alguna dice [EMPTY] es que algo fallo
    # ==========================================================
    print("\n[Verification]")
    for table_name in ["countries","categories","users","products","product_details","orders","order_items","shipping_regions"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"  {'[OK]' if row_count > 0 else '[EMPTY]'} {table_name}: {row_count} rows")

    cursor.close()
    connection.close()
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
