SELECT 
    cliente_id,
    CASE 
        WHEN score >= 8 THEN 'Alto Valor'
        WHEN score BETWEEN 5 AND 7 THEN 'Medio Valor'
        ELSE 'Bajo Valor'
    END AS segmento,
    score,
    ingresos,
    frecuencia_compra
FROM clientes_rfm
ORDER BY segmento, score DESC;
