-- this query is not over. I am trying to filter all news in the world, using
-- v2counts and V2Locations. Checking the results, as 1st approach is not bad, 
-- but the filter needs to be improved

-- idea:
-- https://blog.gdeltproject.org/mapping-the-media-a-geographic-lookup-of-gdelts-sources/

SELECT
  EXTRACT (date
  FROM
    PARSE_TIMESTAMP('%Y%m%d%H%M%S',CAST(date AS string))) AS Date,
  ROUND(CAST(SPLIT(V2Tone, ",") [
    OFFSET
      (0)] AS FLOAT64),2) AS Sentiment,
  (CASE
      WHEN V2Themes LIKE "%POLITICAL_TURMOIL%" THEN "agitacion_politica"
      WHEN V2Themes LIKE "%EPU_POLICY%" THEN "policy_uncertainty"
      WHEN V2Themes LIKE "%CRISISLEX_CRISISLEXREC%" THEN "crisislex"
      WHEN V2Themes LIKE "%EPU_POLICY_GOVERNMENT%" THEN "epu_policy_goverment"
      WHEN V2Themes LIKE "%ARMEDCONFLICT%" THEN "conflicto_armado"
      WHEN V2Themes LIKE "%ARREST%" THEN "arresto"
      WHEN V2Themes LIKE "%SCIENCE%" THEN "ciencia"
      WHEN V2Themes LIKE "%PROTEST%" THEN "protestas"
      WHEN V2Themes LIKE "%ECON_STOCKMARKET%" THEN "econ_inversion_bolsa"
      WHEN V2Themes LIKE "%TERROR%" THEN "terrorismo"
      WHEN V2Themes LIKE "%MILITARY%" THEN "ejercito"
      WHEN V2Themes LIKE "%IDEOLOGY%" THEN "ideologia"
      WHEN V2Themes LIKE "%CORRUPTION%" THEN "corrupcion"
      WHEN V2Themes LIKE "%TRANSPARENCY%" THEN "transparencia"
      WHEN V2Themes LIKE "%EXTREMISM%" THEN "extremismo"
      WHEN V2Themes LIKE "%REFUGEES%" THEN "refugiados"
      WHEN V2Themes LIKE "%WB_1235_CENTRAL_BANKS%" THEN "banco_mundial"
      WHEN V2Themes LIKE "%UNEMPLOYMENT%" THEN "desempleo"
      WHEN V2Themes LIKE "%REBELLION%" THEN "rebelion"
      WHEN V2Themes LIKE "%SCANDAL%" THEN "escandalo"
      WHEN V2Themes LIKE "%ECON_INFLATION%" THEN "econ_inflacion"
      WHEN V2Themes LIKE "%SURVEILLANCE%" THEN "vigilancia"
    ELSE
    "otros_temas"
  END
    ) AS news_in_Spain
FROM
  `gdelt-bq.gdeltv2.gkg_partitioned`
WHERE
  v2counts LIKE '%#SP#%'
  and V2Locations like '%#SP#%'
  AND DATE(_PARTITIONTIME) >= "2019-01-01";