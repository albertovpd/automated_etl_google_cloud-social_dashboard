-- go to schedule query
-- select schedule
-- Run on mondays
-- Append to existing related table
-- The same with the other tables

SELECT
  news_in_Spain AS social,
  Date,
  Sentiment
FROM
  `project-test-3105.gdelt_info_filtering.filtered_spanish_news`
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