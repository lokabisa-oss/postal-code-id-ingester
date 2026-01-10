from lo_ingester.http_async import AsyncHttpIngester
from lo_ingester.models import IngestRequest
from lo_ingester import NoopPolicy


POSTAL_ENDPOINT = "https://kodepos.posindonesia.co.id/CariKodepos"


async def fetch_postal_html(keyword: str) -> str:
    ingester = AsyncHttpIngester(policy=NoopPolicy())

    req = IngestRequest(
        method="POST",
        url=POSTAL_ENDPOINT,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        body=f"kodepos={keyword}",  # 👈 STRING, BUKAN dict
    )

    payload = await ingester.ingest(req)
    return payload.body.decode("utf-8", errors="ignore")
