SELECT DISTINCT ON (Inmueble_ID)
  Inmueble_ID,
  Estado AS ultimo_estado,
  Fecha_Actualización AS fecha_ultima
FROM datos_cambio_estados
ORDER BY Inmueble_ID, Fecha_Actualización DESC;
