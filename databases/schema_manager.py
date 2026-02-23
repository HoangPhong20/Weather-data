from pathlib import Path
import psycopg2
from mysql.connector import Error
from config.database_config import get_database_config


# -------------------------------------------------------------------
# PATH
# -------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
MYSQL_FILE_PATH = BASE_DIR / "sql/schemaMySQL.sql"
POSTGRESQL_FILE_PATH = BASE_DIR / "sql/schemaPostgre.sql"


# -------------------------------------------------------------------
# MYSQL
# -------------------------------------------------------------------

def create_mysql_schema(conn, cursor):

    database = "southeast_asia"

    try:
        cursor.execute(f"DROP DATABASE IF EXISTS {database}")
        cursor.execute(f"CREATE DATABASE {database}")
        conn.commit()

        conn.database = database

        with open(MYSQL_FILE_PATH, "r", encoding="utf-8") as f:
            sql_script = f.read()

        commands = [c.strip() for c in sql_script.split(";") if c.strip()]

        for cmd in commands:
            cursor.execute(cmd)

        conn.commit()
        print("MySQL schema created")

    except Error as e:
        conn.rollback()
        raise RuntimeError(f"MySQL schema creation failed: {e}")



def validate_mysql_schema(cursor):

    try:
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]

        if "weather_data" not in tables:
            raise ValueError("weather_data table missing")

        cursor.execute("SELECT 1 FROM weather_data LIMIT 1")
        if not cursor.fetchone():
            raise ValueError("weather_data empty")

        print("MySQL schema validation OK")

    except Error as e:
        raise RuntimeError(f"MySQL validation failed: {e}")


# -------------------------------------------------------------------
# POSTGRES
# -------------------------------------------------------------------

def create_postgresql_database(cursor, connection, database_name):

    try:
        connection.set_session(autocommit=True)

        cursor.execute(
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{database_name}'"
        )
        cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
        cursor.execute(f"CREATE DATABASE {database_name}")

        print(f"Database {database_name} created")

    except Exception as e:
        raise RuntimeError(f"Create DB failed: {e}")

    finally:
        cursor.close()
        connection.close()



def create_postgresql_schema(database):

    config = get_database_config()
    conn = None

    try:
        conn = psycopg2.connect(
            host=config["postgres"].host,
            port=config["postgres"].port,
            user=config["postgres"].user,
            password=config["postgres"].password,
            database=database,
        )

        cursor = conn.cursor()

        with open(POSTGRESQL_FILE_PATH, encoding="utf-8") as f:
            sql_script = f.read()

        commands = [c.strip() for c in sql_script.split(";") if c.strip()]

        for cmd in commands:
            cursor.execute(cmd)

        conn.commit()
        print("PostgreSQL schema created")

        return conn, cursor

    except Exception as e:
        if conn:
            conn.rollback()
        raise RuntimeError(f"Postgres schema failed: {e}")



def validate_postgresql_schema(cursor):

    try:
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
        """)

        tables = [row[0] for row in cursor.fetchall()]

        if "weather_data" not in tables:
            raise ValueError("weather_data table missing")

        cursor.execute("SELECT 1 FROM weather_data LIMIT 1")
        if not cursor.fetchone():
            raise ValueError("weather_data empty")

        print("PostgreSQL schema validation OK")

    except Exception as e:
        raise RuntimeError(f"Postgres validation failed: {e}")
