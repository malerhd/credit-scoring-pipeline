# --- Parámetros ---
PROJECT_ID   = "loopi-470817"
CLIENT_KEY   = "analytics@redhookdata.com"
MONTHS_BACK  = 24  # últimos N meses
TARGET_TABLE = "loopi-470817.gold.scoring_client_minimal"  # proyecto.dataset.tabla

import os, json, base64, pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

# --- Credenciales ---
if os.getenv("SERVICE_ACCOUNT_B64"):
    info = json.loads(base64.b64decode(os.environ["SERVICE_ACCOUNT_B64"]).decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(info)
else:
    creds = service_account.Credentials.from_service_account_file("/path/a/tu/service_account.json")

bq = bigquery.Client(project=PROJECT_ID, credentials=creds)

# --- Crea la tabla destino si no existe (Opción A) ---
def ensure_target_table(bq_client: bigquery.Client, target_table: str):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{target_table}` (
      client_key STRING,
      score FLOAT64
    )
    """
    bq_client.query(ddl).result()

# --- Query: ventas SOLO TN; Meta como costo; GA para UCR si existe ---
sql = f"""
WITH tn AS (
  SELECT client_key, month,
         SUM(gross_revenue) AS purchase_revenue,   -- SOLO TN
         SUM(orders)        AS purchases           -- SOLO TN
  FROM `{PROJECT_ID}.gold.tn_sales_monthly`
  WHERE client_key = @client_key
    AND month >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL @m_back MONTH), MONTH)
  GROUP BY 1,2
),
meta AS (
  SELECT client_key, month, SUM(spend) AS cost_meta
  FROM `{PROJECT_ID}.gold.ads_monthly`
  WHERE client_key = @client_key AND platform = 'meta-ads'
  GROUP BY 1,2
),
ga AS (
  SELECT client_key, month,
         SAFE_DIVIDE(transactions, NULLIF(sessions,0)) AS ucr
  FROM `{PROJECT_ID}.gold.ga_monthly`
  WHERE client_key = @client_key
),
months AS (
  SELECT client_key, month FROM tn
  UNION DISTINCT SELECT client_key, month FROM meta
  UNION DISTINCT SELECT client_key, month FROM ga
)
SELECT
  m.client_key,
  m.month,
  COALESCE(tn.purchase_revenue, 0.0) AS purchase_revenue,
  COALESCE(tn.purchases, 0)          AS purchases,
  COALESCE(ga.ucr, 0.0)              AS user_conv_rate,
  COALESCE(meta.cost_meta, 0.0)      AS cost_meta,
  0.0                                AS cost_google
FROM months m
LEFT JOIN tn   USING (client_key, month)
LEFT JOIN meta USING (client_key, month)
LEFT JOIN ga   USING (client_key, month)
ORDER BY month
"""

job_config = bigquery.QueryJobConfig(query_parameters=[
    bigquery.ScalarQueryParameter("client_key","STRING",CLIENT_KEY),
    bigquery.ScalarQueryParameter("m_back","INT64",MONTHS_BACK),
])

full_df = bq.query(sql, job_config=job_config).result().to_dataframe()

# --- Renombrar a los nombres que usa tu scoring de Colab ---
full_df = full_df.rename(columns={
    "month": "Month",
    "purchase_revenue": "Purchase revenue",
    "purchases": "Purchases",
    "user_conv_rate": "User conversion rate",
    "cost_meta": "Cost_meta",
    "cost_google": "Cost_google",
})

# === Scoring (tu lógica hardcodeada) ===
def apply_monthly_scoring(full_df: pd.DataFrame,
                          seguidores: int = 376000,
                          engagement_rate_redes: float = 0.045) -> pd.DataFrame:
    full_df['Cost_google']         = pd.to_numeric(full_df.get('Cost_google', 0), errors='coerce').fillna(0)
    full_df['Cost_meta']           = pd.to_numeric(full_df.get('Cost_meta', 0), errors='coerce').fillna(0)
    full_df['Purchase revenue']    = pd.to_numeric(full_df.get('Purchase revenue', 0), errors='coerce').fillna(0)
    full_df['Purchases']           = pd.to_numeric(full_df.get('Purchases', 0), errors='coerce').fillna(0)
    full_df['User conversion rate']= pd.to_numeric(full_df.get('User conversion rate', 0), errors='coerce').fillna(0)

    full_df['Cost_total'] = full_df['Cost_google'] + full_df['Cost_meta']
    full_df['CAC']  = full_df.apply(lambda r: r['Cost_total']/r['Purchases'] if r['Purchases']>0 else 0, axis=1)
    full_df['ROAS'] = full_df.apply(lambda r: r['Purchase revenue']/r['Cost_total'] if r['Cost_total']>0 else 0, axis=1)

    benchmark_conversion = 0.006
    benchmark_roas = 2.5
    benchmark_aov = 50000
    benchmark_cac_ideal    = benchmark_aov * 0.10
    benchmark_cac_muy_alto = benchmark_aov * 0.25
    benchmark_ventas = 20_000_000
    min_conversion = 0.005
    min_roas = 1.5
    min_ventas = 5_000_000

    def puntuar_con_benchmark(v, b, minv=0.0):
        if v >= b:  return 1.0
        if v <= minv: return 0.0
        return (v - minv) / (b - minv)

    def puntuar_cac(cac, ideal, maxv):
        if cac <= ideal: return 1.0
        if cac >= maxv:  return 0.0
        return (maxv - cac) / (maxv - ideal)

    full_df['puntaje_conversion'] = full_df['User conversion rate'].apply(
        lambda x: puntuar_con_benchmark(x, benchmark_conversion, min_conversion))
    full_df['puntaje_roas'] = full_df['ROAS'].apply(
        lambda x: puntuar_con_benchmark(x, benchmark_roas, min_roas))
    full_df['puntaje_ventas'] = full_df['Purchase revenue'].apply(
        lambda x: puntuar_con_benchmark(x, benchmark_ventas, min_ventas))
    full_df['puntaje_cac'] = full_df['CAC'].apply(
        lambda x: puntuar_cac(x, benchmark_cac_ideal, benchmark_cac_muy_alto))

    rrss_score = float(np.clip(np.log(seguidores + 1) * engagement_rate_redes, 0, 1))
    full_df['RRSS_score']   = rrss_score
    full_df['puntaje_rrss'] = rrss_score

    full_df['score_benchmark'] = (
        0.35*full_df['puntaje_ventas'] +
        0.20*full_df['puntaje_roas'] +
        0.15*full_df['puntaje_conversion'] +
        0.15*full_df['puntaje_cac'] +
        0.15*full_df['puntaje_rrss']
    )
    full_df['score_benchmark_100'] = full_df['score_benchmark'] * 100

    def riesgo_categoria(s):
        if s < 40:  return 'No elegible - Rechazo o reevaluación manual'
        if s < 60:  return 'Alto riesgo - Financiación limitada, monitoreo'
        if s < 80:  return 'Moderado - Financiación parcial, tasa media'
        return 'Bajo riesgo - Financiación total, tasa baja'

    full_df['category'] = full_df['score_benchmark_100'].apply(riesgo_categoria)
    return full_df

if full_df.empty:
    raise RuntimeError("No hay datos para el rango solicitado.")

scored_df = apply_monthly_scoring(full_df, seguidores=376000, engagement_rate_redes=0.045)

# --- Un único score por cliente: promedio últimos 12 meses (ajustable) ---
LAST_N = 12
score_value = float(scored_df['score_benchmark_100'].tail(LAST_N).mean())

# --- Crear tabla si no existe + upsert mínimo (client_key, score) ---
ensure_target_table(bq, TARGET_TABLE)

# borrar previos del mismo client_key (idempotencia)
bq.query(
    f"DELETE FROM `{TARGET_TABLE}` WHERE client_key = @client_key",
    job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("client_key","STRING",CLIENT_KEY)]
    )
).result()

# insertar
out = pd.DataFrame([{"client_key": CLIENT_KEY, "score": score_value}])
bq.load_table_from_dataframe(out, TARGET_TABLE).result()

print(f"OK → {TARGET_TABLE}: client_key={CLIENT_KEY}, score={score_value:.2f}")
