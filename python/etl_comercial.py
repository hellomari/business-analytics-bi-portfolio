import pandas as pd

ventas = pd.read_csv("ventas.csv")
clientes = pd.read_csv("clientes.csv")

ventas["fecha"] = pd.to_datetime(ventas["fecha"])
ventas = ventas.dropna(subset=["monto"])

df = ventas.merge(clientes, on="cliente_id", how="left")
df.to_csv("ventas_limpias.csv", index=False)

print("ETL completado. Archivo generado: ventas_limpias.csv")
