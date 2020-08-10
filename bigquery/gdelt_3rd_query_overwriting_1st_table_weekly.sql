-- the first query is to generate the whole content. this content is loaded in different
-- tables for the sake of speed in Data Studio, so the important tables are this 
-- dashboard tables.

-- then, once the first big request is done, the main table will be weekly overwritten,
-- to get just the new weekly info. This info will be appended to the important tables.

CREATE OR REPLACE TABLE
  `myproject-mydataset.gdelt_info_filtering.filtered_spanish_news` AS
  -- list of themes http://data.gdeltproject.org/api/v2/guides/LOOKUP-GKGTHEMES.TXT
SELECT
  -- este select está solo para usar el where al final y eliminar los null que se generan en news_in_Spain
  *
FROM (
  SELECT
    EXTRACT (date
    FROM
      PARSE_TIMESTAMP('%Y%m%d%H%M%S',CAST(date AS string))) AS Date,
    SourceCommonName,
    ROUND(CAST(SPLIT(V2Tone, ",") [
      OFFSET
        (0)] AS FLOAT64),2) AS Sentiment,
    (CASE
      --WHEN V2Themes LIKE "%IDEOLOGY%" THEN "ideologia"
      -- WHEN V2Themes LIKE "%CRISISLEX_CRISISLEXREC%" THEN "use_of_social_media_for_crisis_and_disaster_response" --https://blog.gdeltproject.org/crisislex-taxonomies-now-available-in-gkg/
      -- WHEN V2Themes LIKE "%EPU_POLICY_GOVERNMENT%" THEN "epu_policy_goverment"
      --WHEN V2Themes LIKE "%ARMEDCONFLICT%" THEN "conflicto_armado"
      --WHEN V2Themes LIKE "%TRANSPARENCY%" THEN "transparencia"
      --WHEN V2Themes LIKE "%SCANDAL%" THEN "escandalo"
      -- ECONOMICAL
        WHEN V2Themes LIKE "%ECON_STOCKMARKET%" THEN "stock_market"
        WHEN V2Themes LIKE "%EPU_POLICY%" THEN "incertidumbre_economica" -- https://blog.gdeltproject.org/economic-policy-uncertainty-is-driving-economic-uncertainty-in-the-era-of-trump/
        WHEN V2Themes LIKE "%WB_1235_CENTRAL_BANKS%" THEN "banco_mundial"
        WHEN V2Themes LIKE "%UNEMPLOYMENT%" THEN "desempleo"
        WHEN V2Themes LIKE "%ECON_INFLATION%" THEN "inflacion_economica"
        WHEN v2themes LIKE "%ECON_BANKRUPTCY%" THEN "quiebra_economica"
        WHEN V2THEMES LIKE "%WB_1104_MACROECONOMIC_VULNERABILITY_AND_DEBT%" THEN "macroeconomia_deuda_y_vulnerabilidad"
        WHEN V2THEMES LIKE "%WB_2745_JOB_QUALITY_AND_LABOR_MARKET_PERFORMANCE%" THEN "job_quality_&_labor_market_performance"
        WHEN v2themes LIKE "%POVERTY%" THEN "pobreza"
        WHEN v2themes LIKE "%ECON_DEBT" THEN "deuda_publica"
        WHEN v2themes LIKE "%WB_471_ECONOMIC_GROWTH%" THEN "crecimiento_economico"
        WHEN v2themes LIKE "%WB_318_FINANCIAL_ARCHITECTURE_AND_BANKING%" THEN "finanzas_y_bancos"
        WHEN v2themes LIKE "%ECON_OILPRICE%" THEN "precio_petroleo"
        WHEN v2themes LIKE "%WB_625_HEALTH_ECONOMICS_AND_FINANCE%" THEN "prosperidad_economica_y_finanzas"
        WHEN v2themes LIKE "%ECON_FREETRADE%" THEN "libre_comercio"
      -- POLITICAL
        WHEN V2Themes LIKE "%POLITICAL_TURMOIL%" THEN "inestabilidad_politica"
        WHEN V2Themes LIKE "%REBELLION%" THEN "rebelion"
        WHEN V2Themes LIKE "%TRIAL%" THEN "juicio"
        WHEN V2Themes LIKE "%TERROR%" THEN "terrorismo"
        WHEN V2Themes LIKE "%MILITARY%" THEN "ejercito"
        WHEN V2Themes LIKE "%CORRUPTION%" THEN "corrupcion"
        WHEN V2Themes LIKE "%PROTEST%" THEN "protestas"
        WHEN V2Themes LIKE "%EXTREMISM%" THEN "extremismo"
        WHEN V2Themes LIKE "%REFUGEES%" THEN "refugiados"
        WHEN V2Themes LIKE "%SURVEILLANCE%" THEN "vigilancia"
        WHEN v2themes LIKE "%WB_2024_ANTI_CORRUPTION_AUTHORITIES%" THEN "autoridades_anticorrupcion"
        WHEN v2themes LIKE "%EPU_CATS_NATIONAL_SECURITY%" THEN "seguridad_nacional" -- http://data.gdeltproject.org/dailytrendreport/GDELT_Trend_Report-2019-12-22.pdf
        WHEN v2themes LIKE "%democracy%" THEN "democracia"
        WHEN v2themes LIKE "%WB_2019_ANTI_CORRUPTION_LEGISLATION%" THEN "legislacion_anticorrupcion"
        WHEN v2themes LIKE "%WB_739_POLITICAL_VIOLENCE_AND_CIVIL_WAR%" THEN "violencia_politica_y_guerra_civil"
      -- SOCIAL
        WHEN v2themes LIKE "%WB_2443_RAPE_AND_SEXUAL_VIOLENCE%" THEN "agresion_sexual"
        WHEN v2themes LIKE "%IMMIGRATION%" THEN "inmigracion"
        WHEN V2Themes LIKE "%SCIENCE%" THEN "ciencia"
        WHEN v2themes LIKE "%EDUCATION  %" THEN "educacion"
        WHEN v2themes LIKE "%ENV_CLIMATECHANGE" THEN "cambio_climatico"
        WHEN v2themes LIKE "%ECON_ENTREPRENEURSHIP%" THEN "emprendimiento"
        WHEN v2themes LIKE "%WB_2165_HEALTH_EMERGENCIES%" THEN "emergencia_sanitaria"
        WHEN v2themes LIKE "%ECON_HOUSING_PRICES%" THEN "precio_vivienda"
        WHEN v2themes LIKE "%WB_2167_PANDEMICS" THEN "pandemia"
        WHEN v2themes LIKE "%WB_525_RENEWABLE_ENERG%" THEN "energias_renovables"
        WHEN v2themes LIKE "%ECON_SUBSIDIES%" THEN "subsidios"
        WHEN v2themes LIKE "%DISCRIMINATION_RACE_RACISM%" THEN "racismo"
        WHEN v2themes LIKE "%HEALTH_VACCINATION%" THEN "vacunas"
        WHEN v2themes LIKE "%MEDIA_CENSORSHIP%" THEN "censura_en_medios"
        WHEN V2THEMES LIKE "%TAX_DISEASE%" THEN "enfermedades_muy_infecciosas"  -- https://blog.gdeltproject.org/infectious-disease-mapping-and-early-warning-with-gdelt/
        WHEN v2themes LIKE "%TAX_DISEASE_CORONAVIRUS_INFECTIONS%" THEN "numero_de_contagios_covid%"
        WHEN v2themes LIKE "%TAX_DISEASE_CORONAVIRUS%" THEN "fallecimiento_por_covid"
    END
      ) AS news_in_Spain,
    v2counts,
    v2locations
  FROM
    `gdelt-bq.gdeltv2.gkg_partitioned`
  WHERE
    v2counts LIKE '%#SP#%'
    AND counts LIKE '%#SP#%'
    AND V2Locations LIKE '%#SP#%'
    AND DATE(_PARTITIONTIME) >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) -- this is the part to overwriting weekly
    AND ( SourceCommonName IN (
      SELECT
        spanish_newspapers
      FROM
        `myproject-mydataset.gdelt_info_filtering.spanish_newspapers_SourceCommonName_160620`)))
WHERE
  news_in_Spain IS NOT NULL