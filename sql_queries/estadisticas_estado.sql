
-- -------------------------------------------------------------
-- Análisis de etapas y estados en el histórico de inmuebles
--
-- Este script realiza un análisis detallado de los estados de los inmuebles a partir del histórico de cambios.
-- Indica cuántos leads pasaron por el estado “Revisar Dirección” y cuánto tiempo en promedio tardaron los registros en salir de este estado.
-- Porcentaje de registros que fue descartado
-- Indica el primer estado al que llega cada registro
-- Validación de inconsistencias identificadas en los datos
--
-- Cada sección está documentada y puede ejecutarse de forma independiente.
-- -------------------------------------------------------------

-- Leads que pasaron por el estado “Revisar Dirección” --
SELECT COUNT(DISTINCT Inmueble_ID) AS leads_revisar_direccion
FROM datos_cambio_estados
WHERE Estado = 'Revisar Dirección';


-- Versión compatible con MySQL sin funciones de ventana anidadas
SELECT
  AVG(TIMESTAMPDIFF(MINUTE, t.fecha_inicio, t.fecha_fin)) AS minutos_promedio_salida_revisar_direccion
FROM (
  SELECT
    i.Inmueble_ID,
    i.fecha_inicio,
    MIN(d.Fecha_Actualizacion) AS fecha_fin
  FROM
    (SELECT Inmueble_ID, MIN(Fecha_Actualizacion) AS fecha_inicio
     FROM datos_cambio_estados
     WHERE Estado = 'Revisar Dirección'
     GROUP BY Inmueble_ID) i
  JOIN datos_cambio_estados d
    ON d.Inmueble_ID = i.Inmueble_ID
   AND d.Estado <> 'Revisar Dirección'
   AND d.Fecha_Actualizacion > i.fecha_inicio
  GROUP BY i.Inmueble_ID, i.fecha_inicio
) t;
    

-- Porcentaje de registros descartados --
SELECT
  100.0 * SUM(CASE WHEN Estado = 'Descartado' THEN 1 ELSE 0 END) / COUNT(*) AS porcentaje_descartados
FROM datos_cambio_estados;

-- Primer estado al que llega cada registro --
SELECT
  Inmueble_ID,
  Estado AS primer_estado
FROM (
  SELECT
    Inmueble_ID,
    Estado,
    ROW_NUMBER() OVER (PARTITION BY Inmueble_ID ORDER BY Fecha_Actualizacion ASC) AS rn
  FROM datos_cambio_estados
) t
WHERE rn = 1;

-- Primer estado al que más llegan los registros --
SELECT primer_estado, COUNT(*) AS cantidad
FROM (
  SELECT
    Inmueble_ID,
    Estado AS primer_estado
  FROM (
    SELECT
      Inmueble_ID,
      Estado,
      ROW_NUMBER() OVER (PARTITION BY Inmueble_ID ORDER BY Fecha_Actualizacion ASC) AS rn
    FROM datos_cambio_estados
  ) t
  WHERE rn = 1
) sub
GROUP BY primer_estado
ORDER BY cantidad DESC
LIMIT 1;

-- Inconsistencias detectables en los datos --
-- 1. Fechas fuera de secuencia para el mismo inmueble
SELECT Inmueble_ID, Fecha_Actualizacion, prev_fecha
FROM (
  SELECT
    Inmueble_ID,
    Fecha_Actualizacion,
    LAG(Fecha_Actualizacion) OVER (PARTITION BY Inmueble_ID ORDER BY Fecha_Actualizacion) AS prev_fecha
  FROM datos_cambio_estados
) t
WHERE prev_fecha IS NOT NULL AND Fecha_Actualizacion < prev_fecha;

-- 2. Estados repetidos consecutivos para el mismo inmueble
SELECT Inmueble_ID, Fecha_Actualizacion, Estado, prev_estado
FROM (
  SELECT
    Inmueble_ID,
    Fecha_Actualizacion,
    Estado,
    LAG(Estado) OVER (PARTITION BY Inmueble_ID ORDER BY Fecha_Actualizacion) AS prev_estado
  FROM datos_cambio_estados
) t
WHERE prev_estado = Estado;

-- 3. Registros con campos obligatorios nulos
SELECT *
FROM datos_cambio_estados
WHERE Inmueble_ID IS NULL OR Estado IS NULL OR Fecha_Actualizacion IS NULL;
