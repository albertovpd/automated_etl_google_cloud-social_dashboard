CREATE OR REPLACE TABLE
  your_table_name.videocall_searches AS
SELECT
  keyword AS videocall_searches,
  date,
  trend_index
FROM
  `your created dataset in bigquery`
WHERE
  ( keyword = "zoom"
    OR keyword = "teams"
    OR keyword = "skype"
    OR keyword = "hangouts"
    OR keyword ="teletrabajo"
    OR keyword ="videollamada"
    OR keyword ="videoconferencia");
SELECT
  *
FROM
  your_table_name.videocall_searches;
CREATE OR REPLACE TABLE
  your_table_name.messaging_searches AS
SELECT
  keyword AS messaging_searches,
  date,
  trend_index
FROM
  `your created dataset in bigquery`
WHERE
  ( keyword = "whatsapp"
    OR keyword = "telegram"
    OR keyword = "viber"
    OR keyword = "tiktok" );
SELECT
  *
FROM
  your_table_name.messaging_searches;
CREATE OR REPLACE TABLE
  your_table_name.political_searches AS
SELECT
  keyword AS political_searches,
  date,
  trend_index
FROM
  `your created dataset in bigquery`
WHERE
  ( keyword = "refugiados"
    OR keyword = "inmigracion"
    OR keyword = "nacionalismo"
    OR keyword = "corrupcion"
    OR keyword ="juicio"
    OR keyword = "guerra comercial") ;
SELECT
  *
FROM
  your_table_name.political_searches;
CREATE OR REPLACE TABLE
  your_table_name.health_searches AS
SELECT
  keyword AS health_searches,
  date,
  trend_index
FROM
  `your created dataset in bigquery`
WHERE
  ( keyword = "coronavirus"
    OR keyword = "pandemia"
    OR keyword = "infeccion"
    OR keyword = "medico");
SELECT
  *
FROM
  your_table_name.health_searches;
CREATE OR REPLACE TABLE
  your_table_name.leisure_searches AS
SELECT
  keyword AS leisure_searches,
  date,
  trend_index
FROM
  `your created dataset in bigquery`
WHERE
  ( keyword = "amazon"
    OR keyword = "netflix"
    OR keyword = "hbo"
    OR keyword = "rakuten"
    OR keyword ="steam");
SELECT
  *
FROM
  your_table_name.leisure_searches;
CREATE OR REPLACE TABLE
  your_table_name.city_searches AS
SELECT
  keyword AS city_searches,
  date,
  trend_index
FROM
  `your created dataset in bigquery`
WHERE
  ( keyword = "cabify"
    OR keyword = "taxi"
    OR keyword = "glovo"
    OR keyword = "just eat"
    OR keyword ="deliveroo"
    OR keyword ="uber eats");
SELECT
  *
FROM
  your_table_name.city_searches;
CREATE OR REPLACE TABLE
  your_table_name.habit_searches AS
SELECT
  keyword AS habit_searches,
  date,
  trend_index
FROM
  `your created dataset in bigquery`
WHERE
  ( keyword = "comida a domicilio"
    OR keyword = "hacer deporte"
    OR keyword = "yoga"
    OR keyword = "meditacion"
    OR keyword ="cursos online");
SELECT
  *
FROM
  your_table_name.habit_searches;