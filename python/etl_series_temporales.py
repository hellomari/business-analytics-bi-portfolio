import pandas as pd
import numpy as np

# Carga de datos
df = pd.read_csv("series.csv")
df.columns = df.columns.str.lower().str.strip()

# Fechas
df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
df = df.dropna(subset=["fecha"])

# Orden
df = df.sort_values("fecha")

# Serie completa
fecha_min = df["fecha"].min()
fecha_max = df["fecha"].max()
rango_fechas = pd.DataFrame({"fecha": pd.date_range(fecha_min, fecha_max, freq="D")})

df = rango_fechas.merge(df, on="fecha", how="left")

# Imputación
df["valor"] = df["valor"].astype(float)
df["valor"] = df["valor"].interpolate(method="linear")
df["valor"] = df["valor"].fillna(method="bfill").fillna(method="ffill")

# Outliers
q1 = df["valor"].quantile(0.25)
q3 = df["valor"].quantile(0.75)
iqr = q3 - q1
lim_inf = q1 - 1.5 * iqr
lim_sup = q3 + 1.5 * iqr

df["valor"] = np.where(df["valor"] < lim_inf, lim_inf, df["valor"])
df["valor"] = np.where(df["valor"] > lim_sup, lim_sup, df["valor"])

# Días hábiles
df["dia_semana"] = df["fecha"].dt.dayofweek
df["es_habil"] = df["dia_semana"] < 5

promedio_habil = df[df["es_habil"]]["valor"].mean()
promedio_no_habil = df[~df["es_habil"]]["valor"].mean()

df["ajuste_habil"] = np.where(df["es_habil"], promedio_habil, promedio_no_habil)

# Agregación semanal
df_semanal = (
    df.groupby(pd.Grouper(key="fecha", freq="W"))
    .agg({"valor": "mean"})
    .reset_index()
)

# Agregación mensual
df_mensual = (
    df.groupby(pd.Grouper(key="fecha", freq="M"))
    .agg({"valor": "mean"})
    .reset_index()
)

# Exportaciones
df.to_csv("series_limpias_diario.csv", index=False)
df_semanal.to_csv("series_limpias_semanal.csv", index=False)
df_mensual.to_csv("series_limpias_mensual.csv", index=False)

print("ETL completado.")
