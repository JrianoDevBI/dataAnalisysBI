WITH zonal_avg AS (
  SELECT Zona, AVG(Precio_Solicitado) AS avg_precio
  FROM datos_muestra
  GROUP BY Zona
)
SELECT
  dm.Id,
  dm.Ciudad,
  dm.Zona,
  dm."Lote Id" AS Lote,
  dm."Precio Solicitado",
  dm."Área",
  ABS(dm."Precio Solicitado" - za.avg_precio) AS diff_vs_zona,
  dm."Precio Solicitado" / dm."Área" AS precio_m2,
  RANK() OVER (ORDER BY dm."Precio Solicitado"/dm."Área" DESC) AS ranking_m2
FROM datos_muestra dm
JOIN zonal_avg za ON dm.Zona = za.Zona
WHERE LOWER(dm."Tipo Inmueble") = 'apartamento';
