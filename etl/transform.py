# transform.py (dentro de etl/)

import logging
from prefect import task
from datetime import datetime
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

@task
def transform_jobs(jobs):
    logger.info("INICIO DE TRANSFORMACIÓN DE OFERTAS DE LINKEDIN")
    seen_links = set()
    transformed = []

    for job in jobs:
        title    = job.get("title")
        link     = job.get("link")
        raw_date = job.get("date")

        # Filtrar si falta título o link
        if not title or not link:
            continue

        # Validación básica de URL
        parsed = urlparse(link)
        if not (parsed.scheme in ("http", "https") and parsed.netloc):
            logger.debug(f"URL inválida descartada: {link}")
            continue

        # Parseo de fecha (asumimos ISO: '2025-05-30T...') → yyyy-mm-dd
        date_obj = None
        if raw_date:
            try:
                date_obj = datetime.strptime(raw_date.split("T")[0], "%Y-%m-%d").date()
            except Exception:
                logger.debug(f"Formato de fecha no reconocido: {raw_date}")
                date_obj = None

        # Evitar duplicados con base en el link
        if link in seen_links:
            continue
        seen_links.add(link)

        transformed.append({
            "title":    title,
            "location": job.get("location"),
            "link":     link,
            "date":     date_obj
        })

    logger.info(f"TRANSFORMACIÓN FINALIZADA. Total único: {len(transformed)} ofertas.")
    return transformed
