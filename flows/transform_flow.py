from __future__ import annotations

from prefect import flow, task, get_run_logger
from google.cloud import storage, bigquery
from google.oauth2 import service_account

import pandas as pd
import json, os, base64, re
from urllib.parse import urlparse
from datetime import datetime, timezone


# ─────────────────────────────────────────────────────────
# Credenciales tolerantes (SERVICE_ACCOUNT_B64 o ADC)
# ─────────────────────────────────────────────────────────
def _get_gcp_credentials():
    """
    Si existe SERVICE_ACCOUNT_B64:
      - Acepta JSON crudo o Base64 (con padding corregido).
    Si no existe, devuelve None para usar ADC.
    """
    raw = (os.getenv("SERVICE_ACCOUNT_B64") or "").strip()
    if not raw:
        return None  # ADC

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


# ─────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────
def _parse_gcs_uri(gcs_path: str) -> tuple[str, str]:
    """gs://bucket/path/file.json -> (bucket, 'path/file.json')"""
    u = urlparse(gcs_path)
    return u.netloc, u.path.lstrip("/")


def _to_date_yyyy_mm_dd(s: str):
    if not s:
        return None
    s = str(s).strip()
    # 1) GA: 'YYYYMMDD'
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d").date()

    # 2) Normalizaciones comunes
    s_norm = (
        s.replace("Z", "+00:00")
         .replace("+0000", "+00:00")
         .replace(".000+00:00", "+00:00")
         .replace(".000Z", "+00:00")
    )
    if " " in s_norm and "T" not in s_norm:
        s_norm = s_norm.replace(" ", "T", 1)

    try:
        return datetime.fromisoformat(s_norm).date()
    except ValueError:
        from email.utils import parsedate_to_datetime
        try:
            return parsedate_to_datetime(s).date()
        except Exception:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()


def _month_floor(d: datetime.date) -> datetime.date:
    return datetime(d.year, d.month, 1, tzinfo=timezone.utc).date()


def _ym_to_date_first(ym: str):
    ym = str(ym or "")
    if len(ym) < 6 or not ym.isdigit():
        return None
    return datetime(int(ym[:4]), int(ym[4:6]), 1, tzinfo=timezone.utc).date()


# ─────────────────────────────────────────────────────────
# IO
# ─────────────────────────────────────────────────────────
@task
def read_json(gcs_path: str) -> dict:
    bucket, blob_path = _parse_gcs_uri(gcs_path)
    creds = _get_gcp_credentials()
    sc = storage.Client(credentials=creds) if creds else storage.Client()
    content = sc.bucket(bucket).blob(blob_path).download_as_text()
    return json.loads(content)


# ─────────────────────────────────────────────────────────
# Parsers existentes
# ─────────────────────────────────────────────────────────
@task
def parse_meta_ads_monthly(payload: dict, client_key: str, platform: str) -> pd.DataFrame:
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
    daily = payload.get("series", {}).get("daily", [])
    if not daily:
        return pd.DataFrame()

    df = pd.DataFrame(daily)
    df["date"] = df["date"].astype(str).str[:8]
    df["date"] = df["date"].apply(_to_date_yyyy_mm_dd)
    df["month"] = df["date"].apply(_month_floor)

    numeric_cols = ["sessions", "transactions", "purchaseRevenue", "averagePurchaseRevenue", "purchaseConversionRate"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    g = df.groupby("month", as_index=False).agg(
        sessions=("sessions", "sum"),
        transactions=("transactions", "sum"),
        purchaseRevenue=("purchaseRevenue", "sum")
    )
    g["averagePurchaseRevenue"] = g.apply(
        lambda row: (row["purchaseRevenue"] / row["transactions"]) if row["transactions"] else 0.0, axis=1
    )
    g["purchaseConversionRate"] = g.apply(
        lambda row: (row["transactions"] / row["sessions"]) if row["sessions"] else 0.0, axis=1
    )

    g.insert(0, "platform", platform)
    g.insert(0, "client_key", client_key)
    return g


@task
def parse_tn_sales_to_monthly(payload: dict, client_key: str, platform: str, only_paid: bool = False) -> pd.DataFrame:
    ventas = payload.get("ventas", [])
    if not ventas:
        vd = payload.get("ventas_diarias", {})
        if isinstance(vd, dict):
            ventas = [o for _, arr in vd.items() for o in (arr or [])]

    if not ventas:
        return pd.DataFrame()

    rows = []
    for v in ventas:
        created = v.get("created_at")
        order_date = _to_date_yyyy_mm_dd(created)
        if not order_date:
            continue
        if only_paid and str(v.get("payment_status", "")).lower() != "paid":
            continue

        month = _month_floor(order_date)
        currency = (v.get("currency") or "NA")
        total = float(v.get("total", 0) or 0)
        subtotal = float(v.get("subtotal", 0) or 0)
        discount = float(v.get("discount", 0) or 0) + float(v.get("discount_gateway", 0) or 0)
        paid = 1 if str(v.get("payment_status", "")).lower() == "paid" else 0

        rows.append({
            "client_key": client_key,
            "platform": platform,
            "month": month,
            "orders": 1,
            "gross_revenue": total,
            "subtotal": subtotal,
            "discount": discount,
            "currency": currency,
            "paid": paid,
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
        lambda r: (r["gross_revenue"] / r["orders"]) if r["orders"] else 0.0,
        axis=1
    )
    return g


# ─────────────────────────────────────────────────────────
# NEW: Parser Instagram / Social metrics
# ─────────────────────────────────────────────────────────
@task
def parse_ig_metrics(payload: dict, client_key: str, platform: str) -> pd.DataFrame:
    def _to_ts(v):
        if not v:
            return None
        s = str(v).strip().replace("Z", "+00:00").replace("+0000", "+00:00")
        try:
            return pd.to_datetime(s, utc=True)
        except Exception:
            return None

    row = {
        "client_key": client_key,
        "platform": platform,  # 'ig'
        "fetched_at": _to_ts(payload.get("fetched_at")),
        "metric_timestamp": _to_ts(payload.get("timestamp")),
        "username": payload.get("username"),
        "followers": int(payload.get("followers") or 0),
        "engagement_rate": float(payload.get("engagementRate") or 0.0),
        "reach": int(payload.get("reach") or 0),
        "impressions": int(payload.get("impressions") or 0),
        "likes": int(payload.get("likes") or 0),
        "views": int(payload.get("views") or 0),
        "profile_views": int(payload.get("profile_views") or 0),
        "accounts_engaged": int(payload.get("accounts_engaged") or 0),
        "total_interactions": int(payload.get("total_interactions") or 0),
        "media_count": int(payload.get("media_count") or 0),
        "updated_at": pd.Timestamp.now(tz="UTC"),
    }
    if row["fetched_at"] is None:
        row["fetched_at"] = row["metric_timestamp"] or pd.Timestamp.now(tz="UTC")
    return pd.DataFrame([row])


# ─────────────────────────────────────────────────────────
# NEW: BCRA -> métricas 12m (desde 'historicas')
# ─────────────────────────────────────────────────────────
@task
def parse_bcra_to_metrics(payload: dict, client_key: str, platform: str) -> pd.DataFrame:
    hist = (payload or {}).get("historicas", {}) or {}
    periods = hist.get("periodos", []) or []
    fetched_raw = (payload or {}).get("fetched_at")
    fetched_at = pd.to_datetime(str(fetched_raw).replace("Z","+00:00"), utc=True, errors="coerce")

    if not periods:
        return pd.DataFrame()

    latest_ym = max(str(p.get("periodo")) for p in periods if p.get("periodo"))
    # Ventana 12m anclada al último período
    want = set()
    y = int(latest_ym[:4]); m = int(latest_ym[4:6])
    for i in range(12):
        yy, mm = y, m - i
        while mm <= 0:
            yy -= 1; mm += 12
        want.add(f"{yy:04d}{mm:02d}")

    per_period: dict[str, list[dict]] = {}
    for p in periods:
        ym = str(p.get("periodo"))
        if ym in want:
            per_period.setdefault(ym, []).extend(p.get("entidades", []) or [])

    worst = None
    months_bad = 0
    entities = set()

    for ym, ents in per_period.items():
        worst_m = 0
        bad = False
        for e in ents:
            sit = int(e.get("situacion", 0) or 0)
            ent_name = str(e.get("entidad") or "")
            if sit >= 1:
                entities.add(ent_name)
                worst_m = max(worst_m, sit)
                if sit >= 3:
                    bad = True
        if worst_m > 0:
            worst = worst_m if worst is None else max(worst, worst_m)
        if bad:
            months_bad += 1

    if worst is None:
        worst = 1  # sin deuda reportada en 12m

    out = pd.DataFrame([{
        "client_key": client_key,
        "platform": platform,  # 'bcra'
        "as_of_month": _ym_to_date_first(latest_ym),
        "fetched_at": fetched_at,
        "worst_situation_12m": int(worst),
        "months_bad_12m": int(months_bad),
        "entity_count_12m": int(len(entities)),
    }])
    return out


# ─────────────────────────────────────────────────────────
# NEW: Merchant -> métrica básica (status + inventario)
# ─────────────────────────────────────────────────────────
@task
def parse_merchant_to_metrics(payload: dict, client_key: str, platform: str) -> pd.DataFrame:
    logger = get_run_logger()

    fetched_raw = (payload or {}).get("fetched_at")
    end_raw = (payload or {}).get("endDate")
    fetched_at = pd.to_datetime(str(fetched_raw).replace("Z","+00:00"), utc=True, errors="coerce")
    as_of_date = _to_date_yyyy_mm_dd(end_raw) or (fetched_at.date() if pd.notnull(fetched_at) else datetime.utcnow().date())

    products = (payload or {}).get("metrics", {}).get("products", {})
    if isinstance(products, dict):
        products = products.get("products") or products.get("items") or []
    elif isinstance(products, list):
        products = products
    else:
        products = (payload or {}).get("products") or (payload or {}).get("items") or []

    feed_products = [p for p in (products or []) if str(p.get("source", "")).lower() == "feed"]

    seen = set()
    dedup = []
    for p in feed_products:
        key = str(p.get("offerId") or p.get("id") or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        dedup.append(p)
    products = dedup

    n_total = len(products)
    logger.info(f"[MERCHANT] feed únicos detectados: {n_total}  fetched_at={fetched_at} as_of_date={as_of_date}")

    if n_total == 0:
        return pd.DataFrame([{
            "client_key": client_key,
            "platform": platform,
            "as_of_date": as_of_date,
            "fetched_at": fetched_at,
            "n_products": 0,
            "product_status_ok_share": 0.0,
            "inventory_score": 0.0,
            "in_stock_share": 0.0,
            "merchant_basic": 0.0,
        }])

    def has_min_publish_fields(p: dict) -> bool:
        title = str(p.get("title") or "").strip()
        link  = str(p.get("link") or "").strip()
        img   = str(p.get("imageLink") or "").strip()
        pr = p.get("price") or {}
        price_ok = bool(pr.get("value")) and bool(pr.get("currency"))
        return bool(title and link and img and price_ok)

    def availability_ok(p: dict) -> tuple[bool, float]:
        avail = str(p.get("availability") or "").strip().lower()
        if avail == "in stock":
            return True, 1.0
        if avail in {"preorder", "backorder"}:
            return True, 0.5
        return False, 0.0

    def is_approved(p: dict) -> bool:
        issues = p.get("issues") or []
        for it in issues:
            if str(it.get("severity") or "").lower() == "error":
                return False
        dss = p.get("destinationStatuses") or p.get("destinations") or []
        if isinstance(dss, list) and dss:
            for ds in dss:
                if ds.get("approved") is False:
                    return False
                if str(ds.get("status") or "").lower() in {"disapproved", "inactive"}:
                    return False
            return True
        return True

    ok_count = 0
    inv_vals = []
    in_stock_cnt = 0

    for p in products:
        min_ok = has_min_publish_fields(p)
        avail_ok, inv_val = availability_ok(p)
        approved = is_approved(p)
        status_ok = (min_ok and avail_ok and approved)

        if status_ok:
            ok_count += 1
            inv_vals.append(inv_val)
            if inv_val == 1.0:
                in_stock_cnt += 1
        else:
            inv_vals.append(0.0)

    n_approved = ok_count
    product_status_ok_share = n_approved / n_total if n_total else 0.0
    inventory_score = (sum(inv_vals) / n_approved) if n_approved else 0.0
    in_stock_share = (in_stock_cnt / n_approved) if n_approved else 0.0
    merchant_basic = 0.75 * product_status_ok_share + 0.25 * inventory_score

    return pd.DataFrame([{
        "client_key": client_key,
        "platform": platform,
        "as_of_date": as_of_date,
        "fetched_at": fetched_at,
        "n_products": int(n_total),
        "product_status_ok_share": float(product_status_ok_share),
        "inventory_score": float(inventory_score),
        "in_stock_share": float(in_stock_share),
        "merchant_basic": float(merchant_basic),
    }])


# ─────────────────────────────────────────────────────────
# NEW: TikTok (Open/Display API v2) → videos + agregado 30d
# ─────────────────────────────────────────────────────────
@task
def parse_tiktok_openapi(payload: dict, client_key: str, platform: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Espera un payload estilo Open/Display API v2:
      { fetched_at, user_info{open_id}, user_stats{follower_count}, videos:[{id,create_time,duration,view_count,like_count,comment_count,share_count}] }
    Devuelve:
      - df_videos (crudo por video, particionado por fetched_at)
      - df_merchant_30d (agregado 30d + flags)
    """
    import pandas as pd

    def _to_ts(v):
        if not v:
            return None
        s = str(v).strip().replace("Z","+00:00").replace("+0000","+00:00")
        try:
            return pd.to_datetime(s, utc=True)
        except Exception:
            return None

    fetched_at = _to_ts(payload.get("fetched_at")) or pd.Timestamp.now(tz="UTC")
    ui = payload.get("user_info") or {}
    stats = payload.get("user_stats") or {}
    videos = payload.get("videos") or []

    open_id = str(ui.get("open_id") or ui.get("openId") or "")
    follower_count = int(stats.get("follower_count") or stats.get("followers") or 0)

    # Video-level
    vrows = []
    for v in videos:
        vid = str(v.get("id") or v.get("video_id") or "")
        ct  = _to_ts(v.get("create_time") or v.get("publish_time") or v.get("publish_ts"))
        dur = int(v.get("duration") or v.get("duration_sec") or 0)
        views = int(v.get("view_count") or v.get("views") or 0)
        likes = int(v.get("like_count") or v.get("likes") or 0)
        coms  = int(v.get("comment_count") or v.get("comments") or 0)
        shrs  = int(v.get("share_count") or v.get("shares") or 0)
        vrows.append({
            "client_key": client_key, "platform": platform,
            "fetched_at": fetched_at, "open_id": open_id,
            "video_id": vid, "publish_ts": ct,
            "duration_sec": dur, "view_count": views,
            "like_count": likes, "comment_count": coms, "share_count": shrs
        })

    df_videos = pd.DataFrame(vrows)

    # Merchant-level (30d)
    end_ts = fetched_at
    start_ts = end_ts - pd.Timedelta(days=30)

    if df_videos.empty:
        df_merch = pd.DataFrame([{
            "client_key": client_key, "platform": platform,
            "as_of_date": end_ts.date(), "fetched_at": fetched_at, "open_id": open_id,
            "videos_last_30d": 0, "views_30d": 0, "likes_30d": 0, "comments_30d": 0, "shares_30d": 0,
            "er_30d": 0.0, "comment_rate_30d": 0.0, "share_rate_30d": 0.0,
            "posts_last_28d": 0, "consistency_score": 0.0,
            "followers": follower_count, "views_per_follower": 0.0,
            "is_observable_30d": False
        }])
        return df_videos, df_merch

    dv = df_videos[(df_videos["publish_ts"].notnull()) & (df_videos["publish_ts"] >= start_ts) & (df_videos["publish_ts"] <= end_ts)].copy()
    dv["views"] = pd.to_numeric(dv["view_count"], errors="coerce").fillna(0)
    dv["likes"] = pd.to_numeric(dv["like_count"], errors="coerce").fillna(0)
    dv["comments"] = pd.to_numeric(dv["comment_count"], errors="coerce").fillna(0)
    dv["shares"] = pd.to_numeric(dv["share_count"], errors="coerce").fillna(0)

    videos_last_30d = int(dv.shape[0])
    views_30d = int(dv["views"].sum())
    likes_30d = int(dv["likes"].sum())
    comments_30d = int(dv["comments"].sum())
    shares_30d = int(dv["shares"].sum())

    er_30d = float((likes_30d + comments_30d + shares_30d) / views_30d) if views_30d > 0 else 0.0
    comment_rate_30d = float(comments_30d / views_30d) if views_30d > 0 else 0.0
    share_rate_30d = float(shares_30d / views_30d) if views_30d > 0 else 0.0

    # Consistencia (CV de intervalos entre posteos en días)
    dv = dv.sort_values("publish_ts")
    diffs = dv["publish_ts"].diff().dt.total_seconds().div(86400.0)  # días
    if diffs.dropna().shape[0] >= 2:
        mean_int = diffs.mean()
        std_int  = diffs.std(ddof=0)
        cv = (std_int / mean_int) if mean_int and mean_int > 0 else 1.0
        consistency_score = float(max(0.0, min(1.0, 1.0 - cv)))
    else:
        consistency_score = 0.0

    views_per_follower = float(views_30d / follower_count) if follower_count > 0 else 0.0

    is_observable_30d = bool((videos_last_30d >= 3) and (views_30d >= 1000))

    df_merch = pd.DataFrame([{
        "client_key": client_key, "platform": platform,
        "as_of_date": end_ts.date(), "fetched_at": fetched_at, "open_id": open_id,
        "videos_last_30d": videos_last_30d, "views_30d": views_30d, "likes_30d": likes_30d,
        "comments_30d": comments_30d, "shares_30d": shares_30d,
        "er_30d": er_30d, "comment_rate_30d": comment_rate_30d, "share_rate_30d": share_rate_30d,
        "posts_last_28d": videos_last_30d, "consistency_score": consistency_score,
        "followers": follower_count, "views_per_follower": views_per_follower,
        "is_observable_30d": is_observable_30d
    }])

    return df_videos, df_merch


# ─────────────────────────────────────────────────────────
# BigQuery DDL
# ─────────────────────────────────────────────────────────
@task
def ensure_bq_objects(project_id: str):
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)

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

    # Tienda Nube monthly
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

    # Social metrics (Instagram)
    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.gold.social_metrics` (
      client_key STRING,
      platform STRING,                 -- 'instagram' o 'ig'
      fetched_at TIMESTAMP,
      metric_timestamp TIMESTAMP,
      username STRING,
      followers INT64,
      engagement_rate FLOAT64,
      reach INT64,
      impressions INT64,
      likes INT64,
      views INT64,
      profile_views INT64,
      accounts_engaged INT64,
      total_interactions INT64,
      media_count INT64,
      updated_at TIMESTAMP
    )
    PARTITION BY DATE(fetched_at)
    CLUSTER BY client_key, platform
    """).result()

    # BCRA metrics
    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.gold.bcra_metrics` (
      client_key STRING,
      platform STRING,            -- 'bcra'
      as_of_month DATE,
      fetched_at TIMESTAMP,
      worst_situation_12m INT64,
      months_bad_12m INT64,
      entity_count_12m INT64
    )
    PARTITION BY as_of_month
    CLUSTER BY client_key, platform
    """).result()

    # Merchant metrics
    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.gold.merchant_metrics` (
      client_key STRING,
      platform STRING,            -- 'merchant'
      as_of_date DATE,
      fetched_at TIMESTAMP,
      n_products INT64,
      product_status_ok_share FLOAT64,
      inventory_score FLOAT64,
      in_stock_share FLOAT64,
      merchant_basic FLOAT64
    )
    PARTITION BY as_of_date
    CLUSTER BY client_key, platform
    """).result()

    # ── NEW: TikTok crudo por video
    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.gold.tiktok_video_metrics` (
      client_key STRING,
      platform STRING,            -- 'tiktok'
      fetched_at TIMESTAMP,
      open_id STRING,
      video_id STRING,
      publish_ts TIMESTAMP,
      duration_sec INT64,
      view_count INT64,
      like_count INT64,
      comment_count INT64,
      share_count INT64
    )
    PARTITION BY DATE(fetched_at)
    CLUSTER BY client_key, platform
    """).result()

    # ── NEW: TikTok agregado 30d por merchant
    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.gold.tiktok_merchant_metrics` (
      client_key STRING,
      platform STRING,            -- 'tiktok'
      as_of_date DATE,
      fetched_at TIMESTAMP,
      open_id STRING,
      videos_last_30d INT64,
      views_30d INT64,
      likes_30d INT64,
      comments_30d INT64,
      shares_30d INT64,
      er_30d FLOAT64,
      comment_rate_30d FLOAT64,
      share_rate_30d FLOAT64,
      posts_last_28d INT64,
      consistency_score FLOAT64,
      followers INT64,
      views_per_follower FLOAT64,
      is_observable_30d BOOL
    )
    PARTITION BY as_of_date
    CLUSTER BY client_key, platform
    """).result()


# ─────────────────────────────────────────────────────────
# BigQuery MERGE helpers
# ─────────────────────────────────────────────────────────
def _merge_table(bq: bigquery.Client, df: pd.DataFrame, target: str, keys: list[str]) -> int:
    if df.empty:
        return 0
    staging = target.replace(".gold.", ".gold._stg_")
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
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)
    return _merge_table(
        bq, df,
        target=f"{project_id}.gold.ads_monthly",
        keys=["client_key", "platform", "month", "campaign_id"]
    )


@task
def upsert_ga_monthly(df: pd.DataFrame, project_id: str) -> int:
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)
    return _merge_table(
        bq, df,
        target=f"{project_id}.gold.ga_monthly",
        keys=["client_key", "platform", "month"]
    )


@task
def upsert_tn_sales_monthly(df: pd.DataFrame, project_id: str) -> int:
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)
    return _merge_table(
        bq, df,
        target=f"{project_id}.gold.tn_sales_monthly",
        keys=["client_key", "platform", "month", "currency"]
    )


@task
def upsert_social_metrics(df: pd.DataFrame, project_id: str) -> int:
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)
    if df.empty:
        return 0

    target = f"{project_id}.gold.social_metrics"
    staging = target.replace(".gold.", ".gold._stg_")

    bq.query(f"CREATE TABLE IF NOT EXISTS `{staging}` LIKE `{target}`").result()
    bq.load_table_from_dataframe(df, staging).result()

    on_cond = "T.client_key=S.client_key AND T.platform=S.platform AND T.fetched_at=S.fetched_at"
    set_cols = [c for c in df.columns if c not in ["client_key", "platform", "fetched_at"]]
    set_clause = ", ".join([f"{c}=S.{c}" for c in set_cols])

    bq.query(f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON {on_cond}
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ROW
    """).result()

    bq.query(f"TRUNCATE TABLE `{staging}`").result()
    return len(df)


@task
def upsert_bcra_metrics(df: pd.DataFrame, project_id: str) -> int:
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)
    return _merge_table(
        bq, df,
        target=f"{project_id}.gold.bcra_metrics",
        keys=["client_key", "platform", "as_of_month"]
    )


@task
def upsert_merchant_metrics(df: pd.DataFrame, project_id: str) -> int:
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)
    return _merge_table(
        bq, df,
        target=f"{project_id}.gold.merchant_metrics",
        keys=["client_key", "platform", "as_of_date"]
    )


# ── NEW: upserts TikTok ──────────────────────────────────
@task
def upsert_tiktok_video_metrics(df: pd.DataFrame, project_id: str) -> int:
    if df.empty: return 0
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)
    target = f"{project_id}.gold.tiktok_video_metrics"
    staging = target.replace(".gold.", ".gold._stg_")
    bq.query(f"CREATE TABLE IF NOT EXISTS `{staging}` LIKE `{target}`").result()
    bq.load_table_from_dataframe(df, staging).result()
    bq.query(f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON T.client_key=S.client_key AND T.platform=S.platform AND T.video_id=S.video_id AND T.fetched_at=S.fetched_at
    WHEN MATCHED THEN UPDATE SET
      open_id=S.open_id, publish_ts=S.publish_ts, duration_sec=S.duration_sec,
      view_count=S.view_count, like_count=S.like_count, comment_count=S.comment_count, share_count=S.share_count
    WHEN NOT MATCHED THEN INSERT ROW
    """).result()
    bq.query(f"TRUNCATE TABLE `{staging}`").result()
    return len(df)

@task
def upsert_tiktok_merchant_metrics(df: pd.DataFrame, project_id: str) -> int:
    if df.empty: return 0
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)
    return _merge_table(
        bq, df,
        target=f"{project_id}.gold.tiktok_merchant_metrics",
        keys=["client_key", "platform", "as_of_date"]
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
      - 'ig'       -> gold.social_metrics
      - 'bcra'     -> gold.bcra_metrics
      - 'merchant' -> gold.merchant_metrics
      - 'tiktok'   -> gold.tiktok_video_metrics + gold.tiktok_merchant_metrics
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

    if p == "ig":
        df = parse_ig_metrics(payload, client_key, p)
        n = upsert_social_metrics(df, project_id)
        logger.info(f"[IG] upsert rows: {n}")
        return n

    if p == "bcra":
        df = parse_bcra_to_metrics(payload, client_key, p)
        if df.empty:
            logger.warning("[BCRA] snapshot sin 'historicas' o sin periodos. No se upserta.")
            return 0
        n = upsert_bcra_metrics(df, project_id)
        logger.info(f"[BCRA] upsert rows: {n}  latest={df.iloc[0]['as_of_month']}")
        return n

    if p == "merchant":
        df = parse_merchant_to_metrics(payload, client_key, p)
        n = upsert_merchant_metrics(df, project_id)
        logger.info(f"[MERCHANT] upsert rows: {n}  as_of={df.iloc[0]['as_of_date']}  n_products={int(df.iloc[0]['n_products'])}")
        return n

    if p == "tiktok":
        df_v, df_m = parse_tiktok_openapi(payload, client_key, p)
        n1 = upsert_tiktok_video_metrics(df_v, project_id)
        n2 = upsert_tiktok_merchant_metrics(df_m, project_id)
        logger.info(f"[TIKTOK] upsert videos: {n1}  upsert merchant: {n2}  observable={bool(df_m.iloc[0]['is_observable_30d'])}")
        return int(n1 + n2)

    msg = f"Plataforma no soportada: {platform}"
    logger.error(msg)
    raise ValueError(msg)
