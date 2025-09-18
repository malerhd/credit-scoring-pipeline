# flows/score_simple_flow.py
from __future__ import annotations
from prefect import flow, task, get_run_logger
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import os, json, base64

# ── Credenciales ──────────────────────────────────────────────────────────────
def _get_gcp_credentials():
    b64 = os.getenv("SERVICE_ACCOUNT_B64")
    if not b64:
        # opcional: usa archivo si preferís
        return service_account.Credentials.from_service_account_file(
            r"C:\keys\loopi-sa.json"  # <- si NO usás SERVICE_ACCOUNT_B64, poné tu ruta real
        )
    info = json.loads(base64.b64decode(b64).decode("utf-8"))
    return service_account.Credentials.from_service_account_info(info)

# ── Tareas ────────────────────────────────────────────────────────────────────
@task
def ensure_target_table(project_id: str, target_table: str):
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{target_table}` (
      client_key STRING,
      score FLOAT64
    )
    """
    bq.query(ddl).result()

@task
def read_monthly_panel(project_id: str, client_key: str, months_back: int) -> pd.DataFrame:
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds)
    sql = f"""
    WITH tn AS (
      SELECT client_key, month,
             SUM(gross_revenue) AS purchase_revenue,
             SUM(orders)        AS purchases
      FROM `{project_id}.gold.tn_sales_monthly`
      WHERE client_key = @client_key
        AND month >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL @m_back MONTH), MONTH)
      GROUP BY 1,2
    ),
    meta AS (
      SELECT client_key, month, SUM(spend) AS cost_meta
      FROM `{project_id}.gold.ads_monthly`
      WHERE client_key = @client_key AND platform = 'meta-ads'
      GROUP BY 1,2
    ),
    ga AS (
      SELECT client_key, month,
             SAFE_DIVIDE(transactions, NULLIF(sessions,0)) AS ucr
      FROM `{project_id}.gold.ga_monthly`
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
        bigquery.ScalarQueryParameter("client_key","STRING",client_key),
        bigquery.ScalarQueryParameter("m_back","INT64",months_back),
    ])
    df = bq.query(sql, job_config=job_config).result().to_dataframe()
    df = df.rename(columns={
        "month": "Month",
        "purchase_revenue": "Purchase revenue",
        "purchases": "Purchases",
        "user_conv_rate": "User conversion rate",
        "cost_meta": "Cost_meta",
        "cost_google": "Cost_google",
    })
    for c in ["Purchase revenue","Purchases","User conversion rate","Cost_meta","Cost_google"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df

def apply_monthly_scoring(full_df: pd.DataFrame,
                          seguidores: int = 376000,
                          engagement_rate_redes: float = 0.045) -> pd.DataFrame:
    # tu scoring hardcodeado
    full_df['Cost_google']          = pd.to_numeric(full_df.get('Cost_google', 0), errors='coerce').fillna(0)
    full_df['Cost_meta']            = pd.to_numeric(full_df.get('Cost_meta', 0), errors='coerce').fillna(0)
    full_df['Purchase revenue']     = pd.to_numeric(full_df.get('Purchase revenue', 0), errors='coerce').fillna(0)
    full_df['Purchases']            = pd.to_numeric(full_df.get('Purchases', 0), errors='coerce').fillna(0)
    full_df['User conversion rate'] = pd.to_numeric(full_df.get('User conversion rate', 0), errors='coerce').fillna(0)

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
        if v >= b: return 1.0
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
    return full_df

def aggregate_client_score(scored_df: pd.DataFrame,
                           months_to_average: int = 12,
                           method: str = "mean_last_n") -> float:
    if scored_df.empty: return 0.0
    df = scored_df.sort_values("Month")
    if method == "latest":
        return float(df["score_benchmark_100"].iloc[-1])
    return float(df["score_benchmark_100"].tail(months_to_average).mean())

@task
def write_minimal_score(project_id: str, target_table: str, client_key: str, score: float):
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds)
    # upsert simple: borrar previos de ese client_key
    bq.query(f"DELETE FROM `{target_table}` WHERE client_key = @client_key",
             job_config=bigquery.QueryJobConfig(
                 query_parameters=[bigquery.ScalarQueryParameter("client_key","STRING",client_key)]
             )).result()
    out = pd.DataFrame([{"client_key": client_key, "score": float(score)}])
    bq.load_table_from_dataframe(out, target_table).result()

# ── Flow ─────────────────────────────────────────────────────────────────────
@flow(name="score-monthly-simple")
def score_monthly_simple(project_id: str,
                         client_key: str,
                         target_table: str,
                         months_back: int = 24,
                         aggregate_last_n: int = 12,
                         seguidores: int = 376000,
                         engagement_rate_redes: float = 0.045,
                         aggregation_method: str = "mean_last_n") -> float:
    logger = get_run_logger()
    ensure_target_table(project_id, target_table)
    panel = read_monthly_panel(project_id, client_key, months_back)
    if panel.empty:
        logger.warning("No hay datos para el rango solicitado.")
        return 0.0
    scored = apply_monthly_scoring(panel, seguidores=seguidores, engagement_rate_redes=engagement_rate_redes)
    score_value = aggregate_client_score(scored, months_to_average=aggregate_last_n, method=aggregation_method)
    write_minimal_score(project_id, target_table, client_key, score_value)
    logger.info(f"[OK] {client_key} → {target_table} score={score_value:.2f}")
    return score_value

if __name__ == "__main__":
    # correr local (opcional)
    score_monthly_simple(
        project_id="loopi-470817",
        client_key="analytics@redhookdata.com",
        target_table="loopi-470817.gold.scoring_client_minimal",
        months_back=24,
        aggregate_last_n=12
    )
