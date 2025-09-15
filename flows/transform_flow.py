# flows/transform_flow.py
from __future__ import annotations
from prefect import flow, task, get_run_logger
from google.cloud import storage, bigquery
import pandas as pd
import json
from urllib.parse import urlparse
from datetime import datetime, timezone
import os, base64
from google.oauth2 import service_account

def _get_gcp_credentials():
    b64 = os.getenv("SERVICE_ACCOUNT_B64")
    if not b64:
        raise RuntimeError("Falta SERVICE_ACCOUNT_B64 en variables de entorno del deployment.")
    info = json.loads(base64.b64decode(b64).decode("utf-8"))
    return service_account.Credentials.from_service_account_info(info)

# ─────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────
def _parse_gcs_uri(gcs_path: str) -> tuple[str, str]:
    """gs://bucket/path/file.json -> (bucket, 'path/file.json')"""
    u = urlparse(gcs_path)
    return u.netloc, u.path.lstrip("/")

def _to_date_yyyy_mm_dd(s: str) -> datetime.date:
    """Acepta 'YYYY-MM-DD' o 'YYYYMMDD' y devuelve date."""
    if not s:
        return None
    s = s.strip()
    if len(s) == 8 and s.isdigit():
        # GA: '20240911'
        return datetime.strptime(s, "%Y%m%d").date()
    return datetime.fromisoformat(s.replace("Z", "+00:00")).date() if ("T" in s or "-" in s) else datetime.strptime(s, "%Y-%m-%d").date()

def _month_floor(d: datetime.date) -> datetime.date:
    return datetime(d.year, d.month, 1, tzinfo=timezone.utc).date()

# ─────────────────────────────────────────────────────────
# IO
# ─────────────────────────────────────────────────────────
@task
def read_json(gcs_path: str) -> dict:
    bucket, blob_path = _parse_gcs_uri(gcs_path)
    client = storage.Client(credentials=_get_gcp_credentials())
    blob = client.bucket(bucket).blob(blob_path)
    content = blob.download_as_text()
    return json.loads(content)




# ─────────────────────────────────────────────────────────
# Parsers
# ─────────────────────────────────────────────────────────
@task
def parse_meta_ads_monthly(payload: dict, client_key: str, platform: str) -> pd.DataFrame:
    """Desanida y agrega por mes/campaña para Meta Ads (metrics → months → days)."""
    rows = []
    for m in payload.get("metrics", []):
        cid = m.get("campaign_id")
        cname = m.get("campaign_name")
        for mm in m.get("months", []):
            month = _to_date_yyyy_mm_dd(mm.get("month"))
            agg = {
                "spend": 0.0,
                "impressions": 0,
                "clicks": 0,
                "inline_link_clicks": 0,
                "unique_inline_link_clicks": 0,
                "website_purchases": 0,
                "website_purchase_conversion_value": 0.0,
            }
            for d in mm.get("days", []):
                agg["spend"] += float(d.get("spend", 0) or 0)
                agg["impressions"] += int(d.get("impressions", 0) or 0)
                agg["clicks"] += int(d.get("clicks", 0) or 0)
                agg["inline_link_clicks"] += int(d.get("inline_link_clicks", 0) or 0)
                agg["unique_inline_link_clicks"] += int(d.get("unique_inline_link_clicks", 0) or 0)
                agg["website_purchases"] += int(d.get("website_purchases", 0) or 0)
                agg["website_purchase_conversion_value"] += float(d.get("website_purchase_conversion_value", 0) or 0)

            rows.append({
                "client_key": client_key,
                "platform": platform,
                "month": month,
                "campaign_id": cid,
                "campaign_name": cname,
                **agg
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ctr"] = (df["clicks"] / df["impressions"]).fillna(0)
    df["roas"] = (df["website_purchase_conversion_value"] / df["spend"]).replace([pd.NA, pd.NaT], 0).fillna(0)
    return df

@task
def parse_ga_to_monthly(payload: dict, client_key: str, platform: str) -> pd.DataFrame:
    """
    GA4: series.daily con fechas 'YYYYMMDD'. Agregamos a mensual:
      sessions, transactions, purchaseRevenue, averagePurchaseRevenue (recalculada), purchaseConversionRate (promedio ponderado por sesiones).
    """
    daily = payload.get("series", {}).get("daily", [])
    if not daily:
        return pd.DataFrame()

    df = pd.DataFrame(daily)
    # Normalizar tipos
    df["date"] = df["date"].astype(str).str[:8]
    df["date"] = df["date"].apply(_to_date_yyyy_mm_dd)
    df["month"] = df["date"].apply(_month_floor)

    numeric_cols = ["sessions", "transactions", "purchaseRevenue", "averagePurchaseRevenue", "purchaseConversionRate"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Agregados mensuales
    g = df.groupby("month", as_index=False).agg(
        sessions=("sessions", "sum"),
        transactions=("transactions", "sum"),
        purchaseRevenue=("purchaseRevenue", "sum")
    )

    # Métricas derivadas
    g["averagePurchaseRevenue"] = g.apply(
        lambda row: (row["purchaseRevenue"] / row["transactions"]) if row["transactions"] else 0.0, axis=1
    )
    # para conversion rate, aproximamos por (sum trans / sum sessions)
    g["purchaseConversionRate"] = g.apply(
        lambda row: (row["transactions"] / row["sessions"]) if row["sessions"] else 0.0, axis=1
    )

    g.insert(0, "platform", platform)
    g.insert(0, "client_key", client_key)
    return g

@task
def parse_tn_sales_to_monthly(payload: dict, client_key: str, platform: str) -> pd.DataFrame:
    """
    Tienda Nube: usamos ventas[]. Agregamos a mensual por 'created_at' (UTC del JSON).
    Métricas: orders, gross_revenue (total), subtotal, discount, currency, avg_order_value.
    """
    ventas = payload.get("ventas", [])
    if not ventas:
        return pd.DataFrame()

    rows = []
    for v in ventas:
        created = v.get("created_at")  # "2025-09-09T23:52:29+0000"
        order_date = _to_date_yyyy_mm_dd(created)
        month = _month_floor(order_date)
        currency = v.get("currency")
        total = float(v.get("total", 0) or 0)
        subtotal = float(v.get("subtotal", 0) or 0)
        discount = float(v.get("discount", 0) or 0) + float(v.get("discount_gateway", 0) or 0)
        payment_status = v.get("payment_status")

        rows.append({
            "client_key": client_key,
            "platform": platform,
            "month": month,
            "orders": 1,
            "gross_revenue": total,
            "subtotal": subtotal,
            "discount": discount,
            "currency": currency,
            "paid": 1 if (str(payment_status).lower() == "paid") else 0,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    g = df.groupby(["client_key", "platform", "month", "currency"], as_index=False).agg(
        orders=("orders", "sum"),
        gross_revenue=("gross_revenue", "sum"),
        subtotal=("subtotal", "sum"),
        discount=("discount", "sum"),
        paid=("paid", "sum"),
    )
    g["avg_order_value"] = g.apply(
        lambda r: (r["gross_revenue"] / r["orders"]) if r["orders"] else 0.0, axis=1
    )
    return g

# ─────────────────────────────────────────────────────────
# BigQuery DDL
# ─────────────────────────────────────────────────────────
@task
def ensure_bq_objects(project_id: str):
    creds = _get_gcp_credentials()  # 👈 usa el helper que agregamos al inicio
    bq = bigquery.Client(project=project_id, credentials=creds)
    # dataset
    bq.query(f"CREATE SCHEMA IF NOT EXISTS `{project_id}.gold`").result()


    # Meta Ads
    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.gold.ads_monthly` (
      client_key STRING,
      platform STRING,
      month DATE,
      campaign_id STRING,
      campaign_name STRING,
      spend FLOAT64,
      impressions INT64,
      clicks INT64,
      inline_link_clicks INT64,
      unique_inline_link_clicks INT64,
      website_purchases INT64,
      website_purchase_conversion_value FLOAT64,
      ctr FLOAT64,
      roas FLOAT64
    )
    PARTITION BY month
    CLUSTER BY client_key, platform
    """).result()

    # GA monthly
    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.gold.ga_monthly` (
      client_key STRING,
      platform STRING,
      month DATE,
      sessions INT64,
      transactions INT64,
      purchaseRevenue FLOAT64,
      averagePurchaseRevenue FLOAT64,
      purchaseConversionRate FLOAT64
    )
    PARTITION BY month
    CLUSTER BY client_key, platform
    """).result()

    # Tienda Nube monthly (ventas)
    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.gold.tn_sales_monthly` (
      client_key STRING,
      platform STRING,
      month DATE,
      currency STRING,
      orders INT64,
      gross_revenue FLOAT64,
      subtotal FLOAT64,
      discount FLOAT64,
      paid INT64,
      avg_order_value FLOAT64
    )
    PARTITION BY month
    CLUSTER BY client_key, platform
    """).result()

# ─────────────────────────────────────────────────────────
# BigQuery MERGE helpers
# ─────────────────────────────────────────────────────────
def _merge_table(bq: bigquery.Client, df: pd.DataFrame, target: str, keys: list[str]) -> int:
    if df.empty:
        return 0
    staging = target.replace(".gold.", ".gold._stg_")
    # Crear staging con el mismo schema
    bq.query(f"CREATE TABLE IF NOT EXISTS `{staging}` LIKE `{target}`").result()
    bq.load_table_from_dataframe(df, staging).result()

    on_cond = " AND ".join([f"T.{k}=S.{k}" for k in keys])
    set_cols = [c for c in df.columns if c not in keys]
    set_clause = ",\n      ".join([f"{c}=S.{c}" for c in set_cols])

    bq.query(f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON {on_cond}
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ROW
    """).result()
    bq.query(f"TRUNCATE TABLE `{staging}`").result()
    return len(df)

@task
def upsert_ads_monthly(df: pd.DataFrame, project_id: str) -> int:
    creds = _get_gcp_credentials()                         # 👈
    bq = bigquery.Client(project=project_id, credentials=creds)  # 👈
    return _merge_table(
        bq, df,
        target=f"{project_id}.gold.ads_monthly",
        keys=["client_key", "platform", "month", "campaign_id"]
    )

@task
def upsert_ga_monthly(df: pd.DataFrame, project_id: str) -> int:
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds)
    return _merge_table(
        bq, df,
        target=f"{project_id}.gold.ga_monthly",
        keys=["client_key", "platform", "month"]
    )

@task
def upsert_tn_sales_monthly(df: pd.DataFrame, project_id: str) -> int:
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds)
    return _merge_table(
        bq, df,
        target=f"{project_id}.gold.tn_sales_monthly",
        keys=["client_key", "platform", "month", "currency"]
    )


# ─────────────────────────────────────────────────────────
# Flow principal
# ─────────────────────────────────────────────────────────
@flow(name="transform-flow")
def transform_flow(gcs_path: str, client_key: str, platform: str, project_id: str) -> int:
    """
    Orquesta la transformación y carga a BQ según plataforma:
      - 'meta-ads' -> gold.ads_monthly
      - 'ga'       -> gold.ga_monthly
      - 'tn'       -> gold.tn_sales_monthly

    Params:
      gcs_path   : gs://loopi-data-dev/<client>/<platform>/snapshot-latest.json
      client_key : identificador lógico del cliente (ej. email o uuid)
      platform   : 'meta-ads' | 'ga' | 'tn'
      project_id : GCP project id destino
    """
    logger = get_run_logger()
    ensure_bq_objects(project_id)
    payload = read_json(gcs_path)

    p = platform.lower().strip()
    if p == "meta-ads":
        df = parse_meta_ads_monthly(payload, client_key, p)
        n = upsert_ads_monthly(df, project_id)
        logger.info(f"[META-ADS] upsert rows: {n}")
        return n

    if p == "ga":
        df = parse_ga_to_monthly(payload, client_key, p)
        n = upsert_ga_monthly(df, project_id)
        logger.info(f"[GA] upsert rows: {n}")
        return n

    if p == "tn":
        df = parse_tn_sales_to_monthly(payload, client_key, p)
        n = upsert_tn_sales_monthly(df, project_id)
        logger.info(f"[TN] upsert rows: {n}")
        return n

    msg = f"Plataforma no soportada: {platform}"
    logger.error(msg)
    raise ValueError(msg)
