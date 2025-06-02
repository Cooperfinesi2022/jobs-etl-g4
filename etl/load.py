# load.py

import os
import logging
from prefect import task
import mysql.connector
from mysql.connector import errorcode

logger = logging.getLogger(__name__)

@task
def load_jobs(jobs):
    logger.info("INICIO DE CARGA DE OFERTAS A MySQL")

    # Leer variables de entorno
    db_host     = os.getenv("DB_HOST", "localhost")
    db_user     = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASS", "root")
    db_name     = os.getenv("DB_NAME", "linkedin")

    try:
        conn = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        cursor = conn.cursor()

        # Creamos la tabla (si no existe) con índice UNIQUE en 'link'
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ofertas_laborales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255),
                location VARCHAR(255),
                link TEXT,
                date DATE,
                UNIQUE (link)
            )
        """)

        insert_query = """
            INSERT IGNORE INTO ofertas_laborales (title, location, link, date)
            VALUES (%s, %s, %s, %s)
        """
        inserted = 0
        for job in jobs:
            cursor.execute(insert_query, (
                job["title"],
                job["location"],
                job["link"],
                job["date"]
            ))
            if cursor.rowcount == 1:
                inserted += 1

        conn.commit()
        logger.info(f"CARGA COMPLETADA. Se insertaron {inserted} ofertas nuevas.")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Credenciales inválidas para MySQL")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error(f"La base de datos '{db_name}' no existe")
        else:
            logger.error(f"Error en MySQL: {err}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
