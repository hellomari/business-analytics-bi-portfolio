SELECT 
    etapa,
    COUNT(*) AS clientes,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS porcentaje_funnel
FROM funnel_etapas
GROUP BY etapa
ORDER BY porcentaje_funnel DESC;
