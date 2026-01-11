from lo_ingester.http_async import AsyncHttpIngester
from lo_ingester.models import IngestRequest

from postal_code_id_ingester.policy.retry_policy import SimpleRetryPolicy


POSTAL_ENDPOINT = "https://kodepos.posindonesia.co.id/CariKodepos"


async def fetch_postal_html(
    keyword: str,
    start: int = 0,
    length: int = 25,
) -> str:
    """
    Fetch postal code HTML results for a keyword with pagination support.
    """
    policy = SimpleRetryPolicy(
        max_attempts=3,
        base_delay=1.0,
    )

    ingester = AsyncHttpIngester(policy=policy)

    body = (
        f"kodepos={keyword}"
        f"&start={start}"
        f"&length={length}"
    )

    req = IngestRequest(
        method="POST",
        url=POSTAL_ENDPOINT,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        body=body,
    )

    payload = await ingester.ingest(req)
    return payload.body.decode("utf-8", errors="ignore")
