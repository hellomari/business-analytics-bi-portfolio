SELECT
    fecha,
    sucursal,
    SUM(monto) AS ventas,
    COUNT(DISTINCT cliente_id) AS clientes_atendidos
FROM ventas
GROUP BY fecha, sucursal
ORDER BY fecha DESC; 
