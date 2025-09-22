# flows/orchestrate_client_flow.py
from __future__ import annotations
from prefect import flow, get_run_logger
from flows.transform_flow import transform_flow
from flows.score_simple_flow import score_monthly_simple

def _path_for(platform: str, client_key: str) -> str:
    base = f"gs://loopi-data-dev/{client_key}"
    return {
        "tn":       f"{base}/tiendanube/snapshot-latest.json",
        "ga":       f"{base}/ga/snapshot-latest.json",
        "meta-ads": f"{base}/meta-ads/snapshot-latest.json",
    }[platform]

@flow(name="orchestrate-client")
def orchestrate_client(
    project_id: str,
    client_key: str,
    platforms: list[str] = ["tn","ga","meta-ads"],
    target_table: str = "loopi-470817.gold.scoring_client_minimal",
    months_back: int = 24,
    aggregate_last_n: int = 12,
    seguidores: int = 376000,
    engagement_rate_redes: float = 0.045,
):
    logger = get_run_logger()

    # 1) Transform solo para las plataformas disponibles
    for p in platforms:
        n = transform_flow(
            gcs_path=_path_for(p, client_key),
            client_key=client_key,
            platform=p,
            project_id=project_id,
        )
        logger.info(f"[{p}] upsert rows: {n}")

    # 2) Scoring
    score = score_monthly_simple(
        project_id=project_id,
        client_key=client_key,
        target_table=target_table,
        months_back=months_back,
        aggregate_last_n=aggregate_last_n,
        seguidores=seguidores,
        engagement_rate_redes=engagement_rate_redes,
    )
    logger.info(f"Score {client_key}: {score}")
    return score
