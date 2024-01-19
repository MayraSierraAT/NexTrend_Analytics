import pandas as pd

# 1 ---------------------------------------------------------------
# Leemos los archivos a unir
df_bars_florida = pd.read_csv("gs://datahouse-pf/data/new/df_bars_florida")
df_tips_florida = pd.read_csv("gs://datahouse-pf/data/new/df_tips_florida")

del df_bars_florida["Unnamed: 0"], df_tips_florida["Unnamed: 0"] # Eliminamos columna

# Hacemos el merged entre los dataframes de origen
KPIs_and_BI = df_tips_florida.merge(df_bars_florida, left_on="business_id", right_on="business_id", how="inner")

# Cambiamos el tipo de dato de la columna "date"
KPIs_and_BI["date"] = KPIs_and_BI["date"].astype("datetime64[ms]")

# Hacemos algunas imputaciones menores
KPIs_and_BI["attributes"] = KPIs_and_BI["attributes"].fillna(KPIs_and_BI["categories"])
KPIs_and_BI["address"] = KPIs_and_BI["address"].fillna("Empty")
KPIs_and_BI["hours"] = KPIs_and_BI["hours"].fillna("Empty")

# Disponibilizamos el dataset
KPIs_and_BI.to_parquet("gs://datahouse-pf/data/new/KPIs_and_BI.parquet")

# 2----------------------------------------------------------
df_reviews_florida = pd.read_csv("gs://datahouse-pf/data/new/df_reviews_florida")
df_users_florida = pd.read_csv("gs://datahouse-pf/data/new/df_users_bars")

# Eliminamos columnas innecesarias
df_reviews_florida = df_reviews_florida.drop(columns=["useful", "cool", "funny"])
del df_reviews_florida["Unnamed: 0"], df_users_florida["Unnamed: 0"], df_users_florida["Unnamed: 0.1"]

# Hacemos el merge
BI_2 = df_reviews_florida.merge(df_users_florida, left_on="user_id", right_on="user_id", how="inner")

# Imputamos algunos campos
BI_2["elite"] = BI_2["elite"].fillna("Empty")
BI_2["friends"] = BI_2["friends"].fillna("Empty")

# Disponibilizamos
BI_2.to_parquet("gs://datahouse-pf/data/new/BI_2.parquet")

# 3-------------------------------------------------------------------------

# Hacemos la lectura de los archivos
df_metadatos = pd.read_csv("gs://datahouse-pf/data/new/df_metadatos_bars_fl")
df_reviews_2 = pd.read_csv("gs://datahouse-pf/data/new/df_reviews_2_bars")

# Hacemos el merge entre ambos archivos
BI_3 = df_reviews_2.merge(df_metadatos, left_on="gmap_id", right_on="gmap_id", how="inner")

# Eliminamos algunas columnas innecesarias
BI_3 = BI_3.drop(columns=["Unnamed: 0_x", "Unnamed: 0_y", "name_y"])

# Generamos el dataframe
BI_3.to_parquet("gs://datahouse-pf/data/new/BI_3.parquet")