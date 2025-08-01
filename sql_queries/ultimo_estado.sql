
-- -------------------------------------------------------------
-- Query: Último Estado de Cada Inmueble
-- Descripción:
--   1. Devuelve el último estado registrado para cada inmueble según la fecha más reciente en el histórico.
--   2. Utiliza una subconsulta para identificar la fecha máxima de actualización por inmueble.
--   3. Une la tabla original con la subconsulta para obtener el estado correspondiente a esa fecha.
-- Buenas prácticas:
--   - Uso de subconsulta para claridad y compatibilidad con MySQL.
--   - Alias claros y consistentes.
--   - Comentarios explicativos en cada sección.
--   - Selección explícita de columnas relevantes.
-- -------------------------------------------------------------

-- Query compatible con MySQL para obtener el último estado de cada inmueble
SELECT t.Inmueble_ID,
       t.Estado AS ultimo_estado,
       t.Fecha_Actualizacion AS fecha_ultima
FROM datos_cambio_estados t
INNER JOIN (
    SELECT Inmueble_ID, MAX(Fecha_Actualizacion) AS max_fecha
    FROM datos_cambio_estados
    GROUP BY Inmueble_ID
) ult
  ON t.Inmueble_ID = ult.Inmueble_ID AND t.Fecha_Actualizacion = ult.max_fecha
ORDER BY t.Fecha_Actualizacion DESC
LIMIT 10000;
