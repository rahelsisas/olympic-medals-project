-- Average medals: host vs non-host
SELECT host_flag, AVG(medals_total)
FROM olympics_final
GROUP BY host_flag;

-- Average medals: host vs non-host
SELECT host_flag, AVG(medals_total)
FROM olympics_final
GROUP BY host_flag;