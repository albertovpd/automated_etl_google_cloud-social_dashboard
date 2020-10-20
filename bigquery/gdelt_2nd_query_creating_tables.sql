-- The 1st query request all info and save it to make a concrete graph in Dashboard
-- This 2nd query create tables dividing the Gdelt info, to make other Graphs by theme


CREATE OR REPLACE TABLE
  `myproject-mydataset.gdelt_info_filtering.dashboard_spanish_news_economical` AS
SELECT
  news_in_Spain AS economical,
  Date,
  Sentiment
FROM
  `myproject-mydataset.gdelt_info_filtering.filtered_spanish_news`
WHERE
  news_in_Spain = "stock_market"
  OR news_in_Spain = "incertidumbre_economica"
  OR news_in_Spain = "banco_mundial"
  OR news_in_Spain = "desempleo"
  OR news_in_Spain ="inflacion_economica"
  OR news_in_Spain ="quiebra_economica"
  OR news_in_Spain ="macroeconomia_deuda_y_vulnerabilidad"
  OR news_in_Spain ="job_quality_&_labor_market_performance"
  OR news_in_Spain = "pobreza"
  OR news_in_Spain = "deuda_publica"
  OR news_in_Spain ="crecimiento_economico"
  OR news_in_Spain = "finanzas_y_bancos"
  OR news_in_Spain ="prosperidad_economica_y_finanzas"
  OR news_in_Spain ="libre_comercio"
  OR news_in_Spain ="precio_petroleo" ;
  -- ------------------------------------
CREATE OR REPLACE TABLE
  `myproject-mydataset.gdelt_info_filtering.dashboard_spanish_news_political` AS
SELECT
  news_in_Spain AS political,
  Date,
  Sentiment
FROM
  `myproject-mydataset.gdelt_info_filtering.filtered_spanish_news`
WHERE
  news_in_Spain = "inestabilidad_politica"
  OR news_in_Spain = "rebelion"
  OR news_in_Spain = "juicio"
  OR news_in_Spain = "terrorismo"
  OR news_in_Spain = "ejercito"
  OR news_in_Spain ="corrupcion"
  OR news_in_Spain ="protestas"
  OR news_in_Spain ="extremismo"
  OR news_in_Spain = "refugiados"
  OR news_in_Spain ="vigilancia"
  OR news_in_Spain = "autoridades_anticorrupcion"
  OR news_in_Spain ="seguridad_nacional"
  OR news_in_Spain ="democracia"
  OR news_in_Spain ="precio_petroleo"
  OR news_in_Spain ="violencia_politica_y_guerra_civil";
  -- -------------------------
CREATE OR REPLACE TABLE
  `myproject-mydataset.gdelt_info_filtering.dashboard_spanish_news_social` AS
SELECT
  news_in_Spain AS social,
  Date,
  Sentiment
FROM
  `myproject-mydataset.gdelt_info_filtering.filtered_spanish_news`
WHERE
  news_in_Spain = "agresion_sexual"
  OR news_in_Spain = "inmigracion"
  OR news_in_Spain = "ciencia"
  OR news_in_Spain = "educacion"
  OR news_in_Spain = "cambio_climatico"
  OR news_in_Spain ="emprendimiento"
  OR news_in_Spain ="emergencia_sanitaria"
  OR news_in_Spain ="precio_vivienda"
  OR news_in_Spain = "energias_renovables"
  OR news_in_Spain ="pandemia"
  OR news_in_Spain = "subsidios"
  OR news_in_Spain ="racismo"
  OR news_in_Spain ="vacunas"
  OR news_in_Spain ="censura_en_medios"
  OR news_in_Spain = "enfermedades_muy_infecciosas"  -- https://blog.gdeltproject.org/infectious-disease-mapping-and-early-warning-with-gdelt/
  OR news_in_Spain = "numero_de_contagios_covid%"
  OR news_in_Spain = "fallecimiento_por_covid";