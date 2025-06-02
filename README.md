# Scraping de Ofertas Laborales en LinkedIn

Este proyecto realiza un proceso ETL (Extract, Transform, Load) para extraer ofertas laborales de LinkedIn y almacenarlas en una base de datos MySQL. Su flujo principal consiste en:

1. **Extracción**  
   Se conecta a la página de búsqueda de empleos en LinkedIn (filtrada por la palabra clave `python`), obtiene el HTML y, mediante BeautifulSoup, identifica cada tarjeta de oferta laboral. Para cada oferta extrae:
   - Título del puesto
   - Ubicación
   - URL de la oferta
   - Fecha de publicación

2. **Transformación**  
   Convierte la fecha a un formato de tipo `DATE` de Python (si es posible) y filtra cualquier registro con datos faltantes o inválidos.

3. **Carga**  
   Inserta cada oferta en la tabla `ofertas_laborales` de MySQL. La tabla se crea automáticamente (si no existe) con las columnas:
   - `id` (INT AUTO_INCREMENT)
   - `title` (VARCHAR)
   - `location` (VARCHAR)
   - `link` (TEXT)
   - `date` (DATE)

A través de Prefect se orquesta todo el pipeline, permitiendo ejecutar cada etapa como tarea independiente. El resultado final es una tabla con todas las ofertas de empleo extraídas y normalizadas para futuros análisis o reportes.
