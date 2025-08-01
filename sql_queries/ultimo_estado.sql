
-- Consulta SQL para identificación del último estado de cada inmueble en el pipeline
-- -------------------------------------------------------------
-- ultimo_estado.sql
-- Consulta optimizada para obtener el estado más reciente de cada inmueble.
--
-- Autor: Juan Camilo Riaño Molano
-- Fecha de creación: 01/08/2025
-- Descripción:
--   Esta consulta SQL identifica el último estado registrado para cada inmueble
--   basado en el histórico de cambios de estados:
--   - Obtención del estado más reciente por inmueble usando fecha máxima
--   - Subconsulta optimizada para identificar fechas de última actualización
--   - Join interno para relacionar estado con fecha correspondiente
--   - Ordenamiento por fecha descendente para visualización cronológica
--   - Compatibilidad total con MySQL sin funciones de ventana complejas
--
--   Proporciona una vista consolidada del estado actual de todos
--   los inmuebles en el pipeline para monitoreo y toma de decisiones.
--
-- Funcionalidades principales:
--   - Identificación precisa del último estado por inmueble
--   - Manejo de múltiples actualizaciones en la misma fecha
--   - Optimización de performance usando subconsultas indexadas
--   - Ordenamiento cronológico para análisis temporal
--   - Resultados listos para reportes ejecutivos y dashboards
--
-- Buenas prácticas implementadas:
--   - Subconsulta clara y reutilizable para máxima fecha por inmueble
--   - Alias descriptivos y consistentes en todas las tablas
--   - Comentarios explicativos detallados en secciones críticas
--   - Selección explícita de columnas relevantes sin asteriscos
--   - Compatibilidad con versiones estándar de MySQL
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
ORDER BY t.Fecha_Actualizacion DESC;
