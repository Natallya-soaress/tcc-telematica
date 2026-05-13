import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "database": "telematica",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}


def conectar():
    return psycopg2.connect(**DB_CONFIG)


def salvar_resultado(driver_id, score, timestamp):
    conn = conectar()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO eventos (driver_id, score, timestamp)
        VALUES (%s, %s, %s)
    """, (driver_id, score, timestamp))

    conn.commit()
    cur.close()
    conn.close()