from __future__ import annotations

# Stdlib
import os
import re
import json
import base64
from urllib.parse import urlparse
from typing import Optional  # <-- agregado

# Third-party
from prefect import flow, get_run_logger
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account

# Internal flows
from flows.transform_flow import transform_flow
from flows.score_simple_flow import score_monthly_simple

# Supported platforms
VALID_PLATFORMS = {"tn", "ga", "meta-ads", "ig", "bcra", "merchant"}


# -----------------------------------------------------------------------------
# GCP CREDENTIALS
# -----------------------------------------------------------------------------
def _get_gcp_credentials():
    """
    Reads SERVICE_ACCOUNT_B64 (raw JSON or Base64).
    If not present → uses ADC (default credentials).
    """
    logger = get_run_logger()
    raw = (os.getenv("SERVICE_ACCOUNT_B64") or "").strip()

    if not raw:
        logger.warning("[AUTH] SERVICE_ACCOUNT_B64 empty, using ADC/default")
        return None

    try:
        if raw.lstrip().startswith("{"):
            info = json.loads(raw)
        else:
            clean = re.sub(r"\s+", "", raw)
            clean += "=" * (-len(clean) % 4)
            info = json.loads(base64.b64decode(clean).decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"SERVICE_ACCOUNT_B64 invalid: {e}")

    logger.info("[AUTH] Using service account from secret")
    return service_account.Credentials.from_service_account_info(info)


# -----------------------------------------------------------------------------
# GCS HELPERS
# -----------------------------------------------------------------------------
def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")


def _path_for(platform: str, client_key: str) -> str:
    base = f"gs://loopi-data-dev/{client_key}"
    return {
        "tn": f"{base}/tiendanube/snapshot-latest.json",
        "ga": f"{base}/ga/snapshot-latest.json",
        "meta-ads": f"{base}/meta-ads/snapshot-latest.json",
        "ig": f"{base}/ig/snapshot-latest.json",
        "bcra": f"{base}/bcra/snapshot-latest.json",
        "merchant": f"{base}/merchant/snapshot-latest.json",
    }[platform]


def _gcs_fingerprint(uri: str, creds) -> str | None:
    bucket, path = _parse_gcs_uri(uri)
    client = storage.Client(credentials=creds) if creds else storage.Client()
    blob = client.bucket(bucket).blob(path)

    if not blob.exists():
        return None

    blob.reload()
    return str(blob.generation)


# -----------------------------------------------------------------------------
# IG JSON LOADER
# -----------------------------------------------------------------------------
def _load_ig_metrics(path: str, creds):
    """
    Loads followers + engagement_rate from IG JSON file (local or GCS).
    """
    if path.startswith("gs://"):
        bucket, bpath = _parse_gcs_uri(path)
        sc = storage.Client(credentials=creds) if creds else storage.Client()
        raw = sc.bucket(bucket).blob(bpath).download_as_text()
    else:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()

    data = json.loads(raw)

    followers = int(data.get("followers") or 0)
    engagement = data.get("engagementRate", data.get("engagement_rate", 0))

    try:
        engagement = float(engagement)
    except:
        engagement = 0.0

    return followers, engagement


# -----------------------------------------------------------------------------
# BIGQUERY CURSORS (avoid re-processing)
# -----------------------------------------------------------------------------
def _read_cursor(bq, project_id, client_key, platform):
    sql = f"""
    SELECT last_generation
    FROM `{project_id}.ops.snapshot_cursor`
    WHERE client_key=@ck AND platform=@pf
    LIMIT 1
    """
    rows = list(
        bq.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("ck", "STRING", client_key),
                    bigquery.ScalarQueryParameter("pf", "STRING", platform),
                ]
            ),
        ).result()
    )
    return rows[0]["last_generation"] if rows else None


def _write_cursor(bq, project_id, client_key, platform, generation):
    sql = f"""
    MERGE `{project_id}.ops.snapshot_cursor` T
    USING (SELECT @ck AS client_key, @pf AS platform, @g AS last_generation,
                  CURRENT_TIMESTAMP() AS last_updated) S
    ON T.client_key=S.client_key AND T.platform=S.platform
    WHEN MATCHED THEN
      UPDATE SET last_generation=S.last_generation, last_updated=S.last_updated
    WHEN NOT MATCHED THEN
      INSERT (client_key, platform, last_generation, last_updated)
      VALUES (S.client_key, S.platform, S.last_generation, S.last_updated)
    """

    bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ck", "STRING", client_key),
                bigquery.ScalarQueryParameter("pf", "STRING", platform),
                bigquery.ScalarQueryParameter("g", "STRING", generation),
            ]
        ),
    ).result()


# -----------------------------------------------------------------------------
# PLATFORM NORMALIZATION
# -----------------------------------------------------------------------------
def _normalize_platforms_arg(platforms):
    if platforms is None:
        return None
    if isinstance(platforms, (list, tuple, set)):
        return [str(p).strip() for p in platforms if str(p).strip()]
    if isinstance(platforms, str):
        s = platforms.strip()
        if s in ("", "[]", "null", "None"):
            return []
        try:
            val = json.loads(s)
            if isinstance(val, list):
                return [str(x).strip() for x in val if str(x).strip()]
            if isinstance(val, str):
                return [val.strip()]
        except:
            return [t.strip() for t in s.replace("\n", ",").split(",") if t.strip()]
    return [str(platforms).strip()]


# -----------------------------------------------------------------------------
# MAIN FLOW
# -----------------------------------------------------------------------------
@flow(name="orchestrate-client")
def orchestrate_client(
    project_id: str,
    client_key: str,
    target_table: str | None = None,
    months_back: int = 24,
    aggregate_last_n: int = 12,
    platforms: list[str] | None = None,
    max_age_minutes: int = 1440,
    ig_metrics_path: Optional[str] = None,  # <-- ahora opcional
):
    logger = get_run_logger()

    # Credentials
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)

    logger.info(f"[START] client={client_key} project={bq.project}")

    # --- IG metrics: opcional/skippeable ---
    # fallback a ENV si no vino por parámetro
    if not ig_metrics_path or not ig_metrics_path.strip():
        ig_metrics_path = (os.getenv("IG_METRICS_PATH") or "").strip()

    seguidores: int = 0
    engagement_rate_redes: float = 0.0

    if ig_metrics_path:
        try:
            seguidores, engagement_rate_redes = _load_ig_metrics(ig_metrics_path, creds)
            logger.info(f"[IG] seguidores={seguidores}, engagement={engagement_rate_redes}")
        except Exception as e:
            logger.warning(f"[IG] Skipping IG metrics (load failed): {e}")
    else:
        logger.info("[IG] Skipping IG metrics: ig_metrics_path not provided")

    # Normalize platforms arg
    platforms = _normalize_platforms_arg(platforms)

    # Autodetect from GCS if empty
    if not platforms:
        detected = []
        for p in VALID_PLATFORMS:
            uri = _path_for(p, client_key)
            if _gcs_fingerprint(uri, creds):
                detected.append(p)
        platforms = detected
        logger.info(f"[AUTODETECT] platforms={platforms}")

    # Filter valid ones
    platforms = [p for p in platforms if p in VALID_PLATFORMS]
    if not platforms:
        logger.warning("No platforms to process. Exiting.")
        return 0

    # Process
    processed_any = False

    for p in platforms:
        uri = _path_for(p, client_key)
        fp = _gcs_fingerprint(uri, creds)

        if not fp:
            logger.warning(f"[{p}] No snapshot found, skipping")
            continue

        last = _read_cursor(bq, project_id, client_key, p)
        if last == fp:
            logger.info(f"[{p}] Snapshot unchanged ({fp}), skipping")
            continue

        try:
            n = transform_flow(
                gcs_path=uri,
                client_key=client_key,
                platform=p,
                project_id=project_id,
            )
            logger.info(f"[{p}] transform rows={n}")
            _write_cursor(bq, project_id, client_key, p, fp)
            processed_any = True

        except NotFound:
            logger.warning(f"[{p}] Not found in GCS, skipping")
        except Exception as e:
            logger.error(f"[{p}] Transform error: {e}")

    # Scoring only if updates
    if processed_any:
        score = score_monthly_simple(
            project_id=project_id,
            client_key=client_key,
            target_table=(
                target_table or f"{project_id}.gold.scoring_client_minimal"
            ),
            months_back=months_back,
            aggregate_last_n=aggregate_last_n,
            seguidores=seguidores,
            engagement_rate_redes=engagement_rate_redes,
        )
        logger.info(f"[SCORE] client={client_key} score={score}")
        return score

    logger.info("No updates → skip scoring.")
    return 0

