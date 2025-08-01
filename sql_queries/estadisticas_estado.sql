SELECT COUNT(DISTINCT Inmueble_ID) AS total_revisar
FROM datos_cambio_estados
WHERE Estado = 'Revisar Dirección';

WITH dur AS (
  SELECT
    Inmueble_ID,
    MIN(Fecha_Actualización) FILTER (WHERE Estado='Revisar Dirección') AS inicio,
    MIN(Fecha_Actualización) FILTER (WHERE Estado<>'Revisar Dirección' AND Fecha_Actualización > MIN(Fecha_Actualización) OVER (PARTITION BY Inmueble_ID)) AS fin
  FROM datos_cambio_estados
  GROUP BY Inmueble_ID
)
SELECT ROUND(AVG(EXTRACT(EPOCH FROM (fin - inicio))/86400),2) AS dias_promedio
FROM dur;

SELECT
  ROUND(
    100.0 * SUM(CASE WHEN Estado = 'Descartado' THEN 1 ELSE 0 END) 
    / COUNT(*), 2
  ) AS pct_descartado
FROM datos_cambio_estados;

SELECT Estado, COUNT(*) AS freq
FROM (
  SELECT 
    Inmueble_ID,
    Estado,
    ROW_NUMBER() OVER (PARTITION BY Inmueble_ID ORDER BY Fecha_Actualización) AS rn
  FROM datos_cambio_estados
) t
WHERE rn = 1
GROUP BY Estado
ORDER BY freq DESC
LIMIT 1;

SELECT Inmueble_ID
FROM (
  SELECT
    Inmueble_ID,
    LAG(Fecha_Actualización) OVER (PARTITION BY Inmueble_ID ORDER BY Fecha_Actualización) AS prev,
    Fecha_Actualización
  FROM datos_cambio_estados
) t
WHERE Fecha_Actualización < prev;
