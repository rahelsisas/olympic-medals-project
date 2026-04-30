-- Olympic Medals Analysis Queries
-- Database: data/processed/olympics.db  Table: olympics_final

-- 1. Host country advantage: average medals for host vs non-host nations
SELECT
    host_flag,
    CASE host_flag WHEN 1 THEN 'Host Nation' ELSE 'Non-Host' END AS host_status,
    ROUND(AVG(medals_total), 2) AS avg_medals,
    COUNT(*) AS n_observations
FROM olympics_final
WHERE host_flag IS NOT NULL
GROUP BY host_flag
ORDER BY host_flag DESC;


-- 2. Top 10 most successful nations (1960–2024)
SELECT
    country,
    SUM(medals_total) AS total_medals,
    SUM(gold) AS total_gold,
    SUM(silver) AS total_silver,
    SUM(bronze) AS total_bronze
FROM olympics_final
WHERE country IS NOT NULL
GROUP BY country
ORDER BY total_medals DESC
LIMIT 10;


-- 3. GDP vs medals: average GDP and medals by decade
--    Provides a proxy for wealth-performance correlation over time.
SELECT
    (year / 10) * 10 AS decade,
    ROUND(AVG(gdp_per_capita), 0) AS avg_gdp_per_capita,
    ROUND(AVG(medals_total), 2) AS avg_medals,
    COUNT(*) AS n_country_years
FROM olympics_final
WHERE gdp_per_capita IS NOT NULL
  AND medals_total IS NOT NULL
GROUP BY decade
ORDER BY decade;


-- 4. Data completeness check: how many records are missing surface area
SELECT
    CASE WHEN surface_area IS NULL THEN 'Missing' ELSE 'Present' END AS surface_area_status,
    COUNT(*) AS n_records
FROM olympics_final
GROUP BY surface_area_status;
