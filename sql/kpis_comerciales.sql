SELECT
    sucursal,
    producto,
    SUM(monto_colocado) AS ventas_totales,
    COUNT(DISTINCT cliente_id) AS clientes_unicos,
    SUM(CASE WHEN estado = 'Aprobado' THEN 1 END) * 1.0 / COUNT(*) AS tasa_aprobacion,
    AVG(monto_colocado) AS ticket_promedio
FROM colocaciones
GROUP BY sucursal, producto
ORDER BY ventas_totales DESC;
