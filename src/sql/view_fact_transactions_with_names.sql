CREATE VIEW fact_transactions_with_names AS
SELECT
  ft.id,
  ft.numero_documento,
  c.nombre_propietario,
  ft.valor_servicio,
  ft.fecha_servicio
FROM fact_transactions ft
JOIN dim_clients c
  ON ft.numero_documento = c.numero_documento;