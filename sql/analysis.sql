-- Average medals: host vs non-host
SELECT host_flag, AVG(medals_total)
FROM olympics_final
GROUP BY host_flag;


-- Top 10 countries by medals
SELECT country, SUM(medals_total)
FROM olympics_final
GROUP BY country
ORDER BY SUM(medals_total) DESC
LIMIT 10;