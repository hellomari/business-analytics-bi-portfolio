import pandas as pd

# 1. Carga de datos
creditos = pd.read_csv("creditos.csv")
creditos.columns = creditos.columns.str.lower().str.strip()

# 2. Fechas
creditos["fecha_otorgamiento"] = pd.to_datetime(
    creditos["fecha_otorgamiento"], errors="coerce"
)
creditos = creditos.dropna(subset=["fecha_otorgamiento"])

# 3. Eliminación de duplicados por cliente y producto
creditos = (
    creditos.sort_values(
        by=["cliente_id", "producto", "fecha_otorgamiento"],
        ascending=[True, True, False],
    )
    .drop_duplicates(subset=["cliente_id", "producto"], keep="first")
)

# 4. Limpieza de valores numéricos
creditos["monto"] = pd.to_numeric(creditos["monto"], errors="coerce")
creditos = creditos[creditos["monto"] > 0]

# 5. Estandarización de sucursales
mapa_sucursal = {
    "SCL": "Santiago",
    "Stgo": "Santiago",
    "StaGO": "Santiago",
    "Antofa": "Antofagasta",
    "Antofa.": "Antofagasta",
}
creditos["sucursal"] = creditos["sucursal"].replace(mapa_sucursal)

# 6. Reglas de calidad y topes
creditos = creditos[creditos["monto"] < 20000000]
creditos = creditos[creditos["plazo_meses"] <= 120]

# 7. Columnas derivadas
creditos["anio"] = creditos["fecha_otorgamiento"].dt.year
creditos["mes"] = creditos["fecha_otorgamiento"].dt.month

creditos["categoria"] = creditos["producto"].apply(
    lambda x: "Consumo"
    if "consumo" in x.lower()
    else "Automotriz"
    if "auto" in x.lower()
    else "Otro"
)

# 8. Exportación
creditos.to_csv("creditos_limpios.csv", index=False)

print("ETL avanzado completado:", len(creditos), "registros finales")
