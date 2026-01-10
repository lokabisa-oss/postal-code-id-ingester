from lo_ingester.http_async import AsyncHttpIngester
from lo_ingester.models import IngestRequest

from postal_code_id_ingester.policy.retry_policy import SimpleRetryPolicy


POSTAL_ENDPOINT = "https://kodepos.posindonesia.co.id/CariKodepos"


async def fetch_postal_html(keyword: str) -> str:
    policy = SimpleRetryPolicy(
        max_attempts=3,
        base_delay=1.0,
    )

    ingester = AsyncHttpIngester(policy=policy)

    req = IngestRequest(
        method="POST",
        url=POSTAL_ENDPOINT,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        body=f"kodepos={keyword}",
    )

    payload = await ingester.ingest(req)
    return payload.body.decode("utf-8", errors="ignore")
