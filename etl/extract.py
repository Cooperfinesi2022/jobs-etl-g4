# extract.py (dentro de etl/)

import logging
from prefect import task
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

@task
def extract_jobs():
    logger.info("INICIO DE EXTRACCIÓN DE OFERTAS DE LINKEDIN")
    base_url = "https://www.linkedin.com/jobs/search/?keywords=data"
    headers = {"User-Agent": "Mozilla/5.0"}
    all_jobs = []

    for page in range(0, 5):
        offset = page * 25
        url = f"{base_url}&start={offset}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning(f"Status {response.status_code} en {url} → deteniendo paginación.")
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            job_cards = soup.select(".base-search-card__info")
            logger.info(f"Página {page+1}: {len(job_cards)} ofertas encontradas.")
            if not job_cards:
                break

            for job_card in job_cards:
                title_tag    = job_card.select_one(".base-search-card__title")
                location_tag = job_card.select_one(".job-search-card__location")
                link_tag     = job_card.select_one(".hidden-nested-link")
                date_tag     = job_card.select_one("time.job-search-card__listdate")

                job = {
                    "title":    title_tag.text.strip() if title_tag else None,
                    "location": location_tag.text.strip() if location_tag else None,
                    "link":     link_tag["href"].strip() if link_tag else None,
                    "date":     date_tag["datetime"] if date_tag and date_tag.has_attr("datetime") else None
                }
                all_jobs.append(job)

        except requests.RequestException as e:
            logger.error(f"Error de red o timeout en {url}: {e}")
            break

    logger.info(f"EXTRACCIÓN FINALIZADA. Total bruto: {len(all_jobs)} ofertas.")
    return all_jobs
