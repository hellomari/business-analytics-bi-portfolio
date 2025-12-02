import pandas as pd
import numpy as np

# Carga de datos
eventos = pd.read_csv("eventos_app_web.csv")
crm = pd.read_csv("crm_usuarios.csv")

eventos.columns = eventos.columns.str.lower().str.strip()
crm.columns = crm.columns.str.lower().str.strip()

# Fechas y orden
eventos["timestamp"] = pd.to_datetime(eventos["timestamp"], errors="coerce")
eventos = eventos.dropna(subset=["timestamp"])
eventos = eventos.sort_values(["user_id", "timestamp"])

# Normalización de eventos
mapa_eventos = {
    "view_product": "view",
    "product_view": "view",
    "add_to_cart": "add_cart",
    "addcart": "add_cart",
    "checkout_start": "checkout",
    "purchase_done": "purchase",
}
eventos["evento"] = eventos["evento"].replace(mapa_eventos)

# Reconstrucción de sesiones
eventos["diff_min"] = eventos.groupby("user_id")["timestamp"].diff().dt.total_seconds() / 60
eventos["nueva_sesion"] = (eventos["diff_min"] > 30) | eventos["diff_min"].isna()
eventos["sesion_id"] = eventos.groupby("user_id")["nueva_sesion"].cumsum()

# Funnel ordenado
orden_funnel = {"view": 1, "add_cart": 2, "checkout": 3, "purchase": 4}
eventos["orden"] = eventos["evento"].map(orden_funnel)
eventos = eventos.dropna(subset=["orden"])

# Una fila por etapa por sesión
funnel_sesion = (
    eventos.groupby(["user_id", "sesion_id", "evento"])
    .agg({"timestamp": "min"})
    .reset_index()
)

# Asegurar orden lógico
funnel_sesion = funnel_sesion.sort_values(["user_id", "sesion_id", "timestamp"])

# Pivot
funnel_pivot = funnel_sesion.pivot_table(
    index=["user_id", "sesion_id"],
    columns="evento",
    values="timestamp",
    aggfunc="min"
).reset_index()

# Conversión real
funnel_pivot["view_flag"] = funnel_pivot["view"].notna().astype(int)
funnel_pivot["add_cart_flag"] = funnel_pivot["add_cart"].notna().astype(int)
funnel_pivot["checkout_flag"] = funnel_pivot["checkout"].notna().astype(int)
funnel_pivot["purchase_flag"] = funnel_pivot["purchase"].notna().astype(int)

# Unión con CRM
funnel = funnel_pivot.merge(crm, on="user_id", how="left")

# Tasas por etapa
metricas = pd.DataFrame({
    "etapa": ["view", "add_cart", "checkout", "purchase"],
    "usuarios": [
        funnel["view_flag"].sum(),
        funnel["add_cart_flag"].sum(),
        funnel["checkout_flag"].sum(),
        funnel["purchase_flag"].sum(),
    ]
})

metricas["conversion"] = (
    metricas["usuarios"] / metricas["usuarios"].iloc[0]
).round(4)

# Exportación
funnel.to_csv("funnel_sesiones_usuarios.csv", index=False)
metricas.to_csv("funnel_metricas.csv", index=False)

print("ETL de funnel digital completado.")
