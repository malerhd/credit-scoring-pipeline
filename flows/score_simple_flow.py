# flows/score_simple_flow.py
from __future__ import annotations
from prefect import flow, task, get_run_logger
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import os, json, base64, re

# ==== Extras ====
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime
import math
# ===============

# ── Credenciales (tolerante) ─────────────────────────────────────────────────
def _get_gcp_credentials():
    """
    Usa SERVICE_ACCOUNT_B64 si está (JSON crudo o Base64 con padding corregido).
    Si no está, cae a ADC/GOOGLE_APPLICATION_CREDENTIALS.
    Devuelve un objeto Credentials o None (para usar ADC).
    """
    raw = (os.getenv("SERVICE_ACCOUNT_B64") or "").strip()
    if not raw:
        return None  # ADC
    if (raw.startswith(("'", '"', "`")) and raw.endswith(raw[0])):
        raw = raw[1:-1].strip()
    try:
        if raw.lstrip().startswith("{"):
            info = json.loads(raw)
        else:
            s = re.sub(r"\s+", "", raw)
            s += "=" * (-len(s) % 4)
            info = json.loads(base64.b64decode(s).decode("utf-8"))
        return service_account.Credentials.from_service_account_info(info)
    except Exception as e:
        raise RuntimeError(f"SERVICE_ACCOUNT_B64 inválido (JSON/Base64): {e}")

# ── Tareas ────────────────────────────────────────────────────────────────────
@task
def ensure_target_table(project_id: str, target_table: str):
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{target_table}` (
      client_key STRING,
      score FLOAT64
    )
    """
    bq.query(ddl).result()

@task
def read_monthly_panel(
    project_id: str,
    client_key: str,
    months_back: int,
    ga_fallback_aov: float = 0.0,   # AOV a usar si GA no trae revenue (0 = no estimar)
) -> pd.DataFrame:
    """
    Panel mensual con prioridad de fuentes:
    TN → GA revenue → GA tx×AOV(GA) → GA tx×ga_fallback_aov → 0
    Soporta gold.ga_daily (date YYYYMMDD) y cae a gold.ga_monthly si no existe.
    """
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)

    daily_sql = f"""
    -- GA DAILY → MONTHLY
    WITH ga_daily AS (
      SELECT
        client_key,
        DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(date AS STRING)), MONTH) AS month,
        SUM(sessions)        AS sessions,
        SUM(transactions)    AS transactions,
        SUM(purchaseRevenue) AS ga_revenue
      FROM `{project_id}.gold.ga_daily`
      WHERE client_key = @client_key
        AND PARSE_DATE('%Y%m%d', CAST(date AS STRING)) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL @m_back MONTH), MONTH)
      GROUP BY 1,2
    ),
    ga AS (
      SELECT
        client_key, month,
        SAFE_DIVIDE(transactions, NULLIF(sessions,0)) AS ucr,
        transactions AS ga_purchases,
        ga_revenue,
        SAFE_DIVIDE(ga_revenue, NULLIF(transactions,0)) AS ga_aov
      FROM ga_daily
    ),
    tn AS (
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
    months AS (
      SELECT client_key, month FROM tn
      UNION DISTINCT SELECT client_key, month FROM ga
      UNION DISTINCT SELECT client_key, month FROM meta
    )
    SELECT
      m.client_key,
      m.month,
      COALESCE(
        tn.purchase_revenue,
        NULLIF(ga.ga_revenue, 0),
        (NULLIF(ga.ga_purchases, 0) * NULLIF(ga.ga_aov, 0)),
        (NULLIF(ga.ga_purchases, 0) * NULLIF(@ga_aov, 0)),
        0.0
      ) AS purchase_revenue,
      COALESCE(tn.purchases, ga.ga_purchases, 0) AS purchases,
      COALESCE(ga.ucr, 0.0)                      AS user_conv_rate,
      COALESCE(meta.cost_meta, 0.0)              AS cost_meta,
      0.0                                        AS cost_google
    FROM months m
    LEFT JOIN tn   USING (client_key, month)
    LEFT JOIN ga   USING (client_key, month)
    LEFT JOIN meta USING (client_key, month)
    ORDER BY month
    """

    monthly_sql = f"""
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
             SAFE_DIVIDE(SUM(transactions), NULLIF(SUM(sessions),0)) AS ucr,
             SUM(transactions)    AS ga_purchases,
             SUM(purchaseRevenue) AS ga_revenue
      FROM `{project_id}.gold.ga_monthly`
      WHERE client_key = @client_key
      GROUP BY 1,2
    ),
    months AS (
      SELECT client_key, month FROM tn
      UNION DISTINCT SELECT client_key, month FROM ga
      UNION DISTINCT SELECT client_key, month FROM meta
    )
    SELECT
      m.client_key,
      m.month,
      COALESCE(
        tn.purchase_revenue,
        NULLIF(ga.ga_revenue, 0),
        (NULLIF(ga.ga_purchases, 0) * NULLIF(@ga_aov, 0)),
        0.0
      ) AS purchase_revenue,
      COALESCE(tn.purchases, ga.ga_purchases, 0) AS purchases,
      COALESCE(ga.ucr, 0.0)                      AS user_conv_rate,
      COALESCE(meta.cost_meta, 0.0)              AS cost_meta,
      0.0                                        AS cost_google
    FROM months m
    LEFT JOIN tn   USING (client_key, month)
    LEFT JOIN ga   USING (client_key, month)
    LEFT JOIN meta USING (client_key, month)
    ORDER BY month
    """

    job_params = [
        bigquery.ScalarQueryParameter("client_key","STRING",client_key),
        bigquery.ScalarQueryParameter("m_back","INT64",months_back),
        bigquery.ScalarQueryParameter("ga_aov","FLOAT64",ga_fallback_aov),
    ]

    try:
        df = bq.query(daily_sql, job_config=bigquery.QueryJobConfig(query_parameters=job_params)).result().to_dataframe()
    except Exception:
        df = bq.query(monthly_sql, job_config=bigquery.QueryJobConfig(query_parameters=job_params)).result().to_dataframe()

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

@task
def read_latest_ig_metrics(project_id: str, client_key: str) -> tuple[int, float]:
    """
    Devuelve (followers, engagement_rate) del último registro IG en gold.social_metrics.
    """
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)
    sql = f"""
    SELECT followers, engagement_rate
    FROM `{project_id}.gold.social_metrics`
    WHERE client_key = @ck
      AND platform IN ('ig','instagram')
    ORDER BY fetched_at DESC
    LIMIT 1
    """
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("ck","STRING", client_key)]
    ))
    rows = list(job.result())
    if not rows:
        raise RuntimeError(f"[IG] No hay métricas en `{project_id}.gold.social_metrics` para client_key={client_key}.")
    followers = int(rows[0]["followers"] or 0)
    engagement = float(rows[0]["engagement_rate"] or 0.0)
    return followers, engagement

def apply_monthly_scoring(full_df: pd.DataFrame,
                          seguidores: Optional[int] = None,
                          engagement_rate_redes: Optional[float] = None,
                          rrss_policy: str = "renormalize") -> pd.DataFrame:
    """
    Score mensual base. IG es opcional:
    - rrss_policy='renormalize': si falta IG, se quita su 15% y se redistribuye.
    - rrss_policy='neutral': si falta IG, se usa 0.5.
    """
    # Casting
    full_df['Cost_google']          = pd.to_numeric(full_df.get('Cost_google', 0), errors='coerce').fillna(0)
    full_df['Cost_meta']            = pd.to_numeric(full_df.get('Cost_meta', 0), errors='coerce').fillna(0)
    full_df['Purchase revenue']     = pd.to_numeric(full_df.get('Purchase revenue', 0), errors='coerce').fillna(0)
    full_df['Purchases']            = pd.to_numeric(full_df.get('Purchases', 0), errors='coerce').fillna(0)
    full_df['User conversion rate'] = pd.to_numeric(full_df.get('User conversion rate', 0), errors='coerce').fillna(0)

    # Derivadas
    full_df['Cost_total'] = full_df['Cost_google'] + full_df['Cost_meta']
    full_df['CAC']  = full_df.apply(lambda r: r['Cost_total']/r['Purchases'] if r['Purchases']>0 else 0, axis=1)
    full_df['ROAS'] = full_df.apply(lambda r: r['Purchase revenue']/r['Cost_total'] if r['Cost_total']>0 else 0, axis=1)

    # Benchmarks
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

    # RRSS opcional
    rrss_disponible = (seguidores is not None) and (engagement_rate_redes is not None)
    if rrss_disponible:
        if engagement_rate_redes > 1:
            raise ValueError(f"[IG] engagement_rate {engagement_rate_redes} > 1 (usa proporción 0–1).")
        if seguidores < 0:
            raise ValueError(f"[IG] seguidores inválido: {seguidores}")
        engagement_rate_redes = float(np.clip(engagement_rate_redes, 0.0, 1.0))
        rrss_score = float(np.clip(np.log(seguidores + 1) * engagement_rate_redes, 0, 1))
    else:
        rrss_score = 0.5 if rrss_policy == "neutral" else 0.0
    full_df['RRSS_score']   = rrss_score
    full_df['puntaje_rrss'] = rrss_score

    # Pesos y renormalización si falta IG (renormalize)
    w = {"ventas": 0.35, "roas": 0.20, "conv": 0.15, "cac": 0.15, "rrss": 0.15}
    if not rrss_disponible and rrss_policy == "renormalize":
        denom = w["ventas"] + w["roas"] + w["conv"] + w["cac"]  # 0.85
        w["ventas"] /= denom; w["roas"] /= denom; w["conv"] /= denom; w["cac"] /= denom; w["rrss"] = 0.0

    full_df['score_benchmark'] = (
        w["ventas"]*full_df['puntaje_ventas'] +
        w["roas"]  *full_df['puntaje_roas'] +
        w["conv"]  *full_df['puntaje_conversion'] +
        w["cac"]   *full_df['puntaje_cac'] +
        w["rrss"]  *full_df['puntaje_rrss']
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
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)
    bq.query(
        f"DELETE FROM `{target_table}` WHERE client_key = @client_key",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("client_key","STRING",client_key)]
        )
    ).result()
    out = pd.DataFrame([{"client_key": client_key, "score": float(score)}])
    bq.load_table_from_dataframe(out, target_table).result()

# ── Overlay: Merchant (básico) ───────────────────────────────────────────────
def compute_merchant_basic(merchant_json: Dict[str, Any]) -> Tuple[Optional[float], Dict[str, Any], List[str]]:
    """
    Devuelve (MerchantBasic ∈ [0,1] o None si no hay productos, debug, flags).
    Regla minimalista:
      - product_status_ok_share: % items con title+link+image y (si source=='feed') price.value+currency
                                 y availability válido {in stock, preorder, backorder}
      - inventory_score: mapea availability a {in_stock:1, preorder/backorder:0.5, otros/vacío:0}
      - MerchantBasic = 0.70*product_status_ok_share + 0.30*inventory_score
    """
    try:
        products = merchant_json.get("metrics", {}).get("products", {}).get("products", []) or []
    except Exception:
        products = []

    n = len(products)
    if n == 0:
        return None, {"reason": "no_products"}, ["mc_missing"]

    ok_count = 0
    inv_vals: List[float] = []
    in_stock_count = 0

    for p in products:
        title = str(p.get("title") or "").strip()
        link  = str(p.get("link") or "").strip()
        img   = str(p.get("imageLink") or "").strip()
        avail = str(p.get("availability") or "").strip().lower()
        source = str(p.get("source") or "").strip().lower()

        has_min = bool(title and link and img)
        has_price = True
        if source == "feed":
            price = p.get("price") or {}
            has_price = bool(price.get("value")) and bool(price.get("currency"))
        avail_ok = avail in {"in stock", "preorder", "backorder"}
        status_ok = has_min and has_price and avail_ok
        ok_count += 1 if status_ok else 0

        if avail == "in stock":
            inv_vals.append(1.0); in_stock_count += 1
        elif avail in {"preorder", "backorder"}:
            inv_vals.append(0.5)
        else:
            inv_vals.append(0.0)

    product_status_ok_share = ok_count / n
    inventory_score = sum(inv_vals) / n if n > 0 else 0.0
    in_stock_share = in_stock_count / n if n > 0 else 0.0

    merchant_basic = 0.70 * product_status_ok_share + 0.30 * inventory_score
    flags: List[str] = []
    if product_status_ok_share < 0.70: flags.append("mc_low_status")
    if in_stock_share < 0.60: flags.append("mc_low_stock")

    debug = {
        "n_products": n,
        "product_status_ok_share": round(product_status_ok_share, 4),
        "inventory_score": round(inventory_score, 4),
        "in_stock_share": round(in_stock_share, 4),
        "merchant_basic": round(merchant_basic, 4),
    }
    return float(merchant_basic), debug, flags

# ── Overlay: BCRA (historicas → 12m) ─────────────────────────────────────────
def _months_back_from_ym(latest_ym: str, months: int = 12) -> Set[str]:
    y = int(latest_ym[:4]); m = int(latest_ym[4:6])
    out = set()
    for i in range(months):
        yy = y; mm = m - i
        while mm <= 0:
            yy -= 1; mm += 12
        out.add(f"{yy:04d}{mm:02d}")
    return out

def bcra_extract_12m_from_historicas(bcra_json: Dict[str, Any]) -> Dict[str, Any]:
    hist = (bcra_json or {}).get("historicas", {}) or {}
    periods = hist.get("periodos", []) or []
    if not periods:
        return {"worst_situation_12m": None, "months_bad_12m": None, "entity_count_12m": 0, "periods_considered": []}

    latest_ym = max(str(p.get("periodo")) for p in periods if p.get("periodo"))
    win = _months_back_from_ym(latest_ym, months=12)

    per_period: Dict[str, List[Dict[str, Any]]] = {}
    for p in periods:
        ym = str(p.get("periodo"))
        if ym in win:
            per_period.setdefault(ym, []).extend(p.get("entidades", []) or [])

    worst = None; months_bad = 0; entities: Set[str] = set()
    for ym, entidades in per_period.items():
        worst_month = 0; bad = False
        for e in entidades:
            sit = int(e.get("situacion", 0) or 0)
            ent_name = str(e.get("entidad") or "")
            if sit >= 1:
                entities.add(ent_name)
                worst_month = max(worst_month, sit)
                if sit >= 3: bad = True
        if worst_month > 0: worst = worst_month if worst is None else max(worst, worst_month)
        if bad: months_bad += 1

    if worst is None: worst = 1  # sin deuda en 12m

    return {
        "as_of_month": latest_ym,
        "worst_situation_12m": int(worst),
        "months_bad_12m": int(months_bad),
        "entity_count_12m": len(entities),
        "periods_considered": sorted(per_period.keys()),
    }

def bcra_to_score_agg(worst_situation_12m: Optional[int],
                      months_bad_12m: Optional[int],
                      base_map: Dict[int, float] = None,
                      months_coef: float = 0.08) -> Tuple[Optional[float], Dict[str, Any], List[str]]:
    if worst_situation_12m is None:
        return None, {"reason": "missing_worst"}, ["bcra_missing"]
    if base_map is None:
        base_map = {1:1.00, 2:0.85, 3:0.55, 4:0.25, 5:0.00, 6:0.00}

    base = base_map.get(int(worst_situation_12m), 0.0)
    months_penalty = 1.0
    if months_bad_12m is not None:
        months_penalty = max(0.5, 1.0 - months_coef * int(months_bad_12m))

    score = max(0.0, min(1.0, base * months_penalty))

    flags: List[str] = []
    if worst_situation_12m >= 5:
        flags.append("bcra_critical")
    elif worst_situation_12m == 4 and (months_bad_12m is not None and months_bad_12m >= 3):
        flags.append("bcra_high_risk")

    dbg = {
        "worst_situation_12m": int(worst_situation_12m),
        "months_bad_12m": int(months_bad_12m or 0),
        "base": base,
        "months_penalty": months_penalty,
        "score_0_1": score
    }
    return float(score), dbg, flags

# ── Blend + Caps ─────────────────────────────────────────────────────────────
def blend_overlay(base01: float,
                  merchant01: Optional[float],
                  bcra01: Optional[float],
                  wM: float = 0.08,
                  wB: float = 0.07) -> float:
    """
    Mezcla convexa con renormalización si falta alguna fuente.
    """
    parts: List[float] = [base01]; weights: List[float] = [max(0.0, 1.0 - wM - wB)]
    if merchant01 is not None: parts.append(merchant01); weights.append(wM)
    if bcra01 is not None:     parts.append(bcra01);     weights.append(wB)
    s = sum(weights)
    if s <= 0: return base01
    weights = [w/s for w in weights]
    return sum(p*w for p, w in zip(parts, weights))

def apply_bcra_overrides(final01: float, bcra_flags: List[str]) -> float:
    """
    Caps por política sobre 0–1.
    """
    if "bcra_critical" in bcra_flags:
        return min(final01, 0.35)  # 35/100
    if "bcra_high_risk" in bcra_flags:
        return min(final01, 0.55)  # 55/100
    return final01

# ── Flow ─────────────────────────────────────────────────────────────────────
@flow(name="score-monthly-simple")
def score_monthly_simple(project_id: str,
                         client_key: str,
                         target_table: str,
                         months_back: int = 24,
                         aggregate_last_n: int = 12,
                         seguidores: int = 0,                 # legacy (no se usa si IG auto)
                         engagement_rate_redes: float = 0.0,  # legacy
                         aggregation_method: str = "mean_last_n",
                         rrss_policy: str = "renormalize",    # 'renormalize' | 'neutral'
                         ga_fallback_aov: float = 0.0,        # AOV para estimar revenue GA si falta
                         merchant_json: Optional[Dict[str, Any]] = None,
                         bcra_json: Optional[Dict[str, Any]] = None,
                         wM: float = 0.08,  # peso Merchant básico
                         wB: float = 0.07   # peso BCRA
                         ) -> float:
    logger = get_run_logger()
    ensure_target_table(project_id, target_table)

    # 1) Panel mensual (TN con fallback GA)
    panel = read_monthly_panel(project_id, client_key, months_back, ga_fallback_aov=ga_fallback_aov)
    if panel.empty:
        raise RuntimeError("No hay datos de panel mensual para el rango solicitado.")

    # 2) IG (NO obligatorio)
    ig_seguidores: Optional[int] = None
    ig_engagement: Optional[float] = None
    try:
        ig_seguidores, ig_engagement = read_latest_ig_metrics(project_id, client_key)
        logger.info(f"[IG] métricas: seguidores={ig_seguidores}, engagement={ig_engagement}")
    except Exception as e:
        logger.warning(f"[IG] no disponible: {e}. Política RRSS='{rrss_policy}'.")

    # 3) Scoring base
    scored = apply_monthly_scoring(
        panel,
        seguidores=ig_seguidores,
        engagement_rate_redes=ig_engagement,
        rrss_policy=rrss_policy
    )
    base_score_value = aggregate_client_score(scored, months_to_average=aggregate_last_n, method=aggregation_method)
    base01 = max(0.0, min(1.0, base_score_value / 100.0))

    # 4) Overlays opcionales
    m_score01: Optional[float] = None; m_dbg: Dict[str, Any] = {}; m_flags: List[str] = []
    if merchant_json is not None:
        m_score01, m_dbg, m_flags = compute_merchant_basic(merchant_json)

    b_score01: Optional[float] = None; b_dbg: Dict[str, Any] = {}; b_flags: List[str] = []
    if bcra_json is not None:
        agg = bcra_extract_12m_from_historicas(bcra_json)
        b_score01, b_dbg, b_flags = bcra_to_score_agg(
            worst_situation_12m=agg.get("worst_situation_12m"),
            months_bad_12m=agg.get("months_bad_12m")
        )

    # 5) Blend + caps
    blended01 = blend_overlay(base01, m_score01, b_score01, wM=wM, wB=wB)
    blended01_capped = apply_bcra_overrides(blended01, b_flags)
    score_value = round(blended01_capped * 100.0, 2)

    # 6) Persistencia
    write_minimal_score(project_id, target_table, client_key, score_value)

    logger.info(
        f"[BASE] {base_score_value:.2f} "
        f"[M] {m_dbg} flags={m_flags} "
        f"[BCRA] {b_dbg} flags={b_flags} "
        f"[FINAL] {score_value:.2f}"
    )

    return score_value

if __name__ == "__main__":
    # correr local (usará ADC si no definís SERVICE_ACCOUNT_B64)
    score_monthly_simple(
        project_id="loopi-470817",
        client_key="analytics@redhookdata.com",
        target_table="loopi-470817.gold.scoring_client_minimal",
        months_back=24,
        aggregate_last_n=12,
        rrss_policy="renormalize",
        ga_fallback_aov=50000,  # si no querés estimar, poné 0.0
        # merchant_json=...,     # dict del Content API
        # bcra_json=...,         # dict de tu servicio BCRA
        wM=0.08,
        wB=0.07
    )
