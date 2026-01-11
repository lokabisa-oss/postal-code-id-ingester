import argparse
import asyncio

from postal_code_id_ingester.ingest.region_id_loader import (
    load_villages_from_region_id
)
from postal_code_id_ingester.ingest.fetcher import fetch_postal_html
from postal_code_id_ingester.sources.pos_indonesia import parse_postal_results
from postal_code_id_ingester.matchers.region_matcher import match_postal_candidate
from postal_code_id_ingester.model.augmented import AugmentedPostalCode
from postal_code_id_ingester.export.jsonl import write_jsonl
from postal_code_id_ingester.export.resume import load_seen_village_codes
from postal_code_id_ingester.query.keywords import extract_single_word

PAGE_SIZE = 25
MAX_PAGES = 5

async def process_village(
    sem: asyncio.Semaphore,
    v,
    verbose: bool = False,
):
    async with sem:
        if verbose:
            print(f"PROCESS {v.village} ({v.village_code})")

        # Keyword strategy (ORDER MATTERS)
        keywords = [
            v.village,                     # 1. default (as-is)
            v.district,                    # 2. fallback district
            extract_single_word(v.village) # 3. fallback single-word village
        ]

        tried = set()

        for keyword in keywords:
            if not keyword:
                continue

            keyword = keyword.strip()
            if not keyword or keyword in tried:
                continue

            tried.add(keyword)

            if verbose:
                print(f"  TRY keyword='{keyword}'")

            for page in range(MAX_PAGES):
                start = page * PAGE_SIZE

                if verbose:
                    print(f"    PAGE start={start}")

                try:
                    html = await fetch_postal_html(
                        keyword,
                        start=start,
                        length=PAGE_SIZE,
                    )
                except Exception as e:
                    if verbose:
                        print(f"    FETCH ERROR keyword={keyword}: {e}")
                    break

                candidates = parse_postal_results(html)
                if not candidates:
                    break  # no more pages

                for c in candidates:
                    score = match_postal_candidate(v, c)
                    if score:
                        if verbose:
                            print(
                                f"    MATCH keyword='{keyword}' "
                                f"postal_code={c['postal_code']} "
                                f"score={round(score, 3)}"
                            )
                        return AugmentedPostalCode(
                            village_code=v.village_code,
                            postal_code=c["postal_code"],
                            source="pos-indonesia",
                            confidence=round(score, 3),
                            retrieved_at=AugmentedPostalCode.now_iso(),
                            raw=c,
                        )

        if verbose:
            print(f"  NO MATCH {v.village}")

        return None


async def run_ingestion(
    regions_path: str,
    output_path: str,
    limit: int | None = None,
    verbose: bool = False,
    concurrency: int = 3,
):
    villages = load_villages_from_region_id(regions_path)

    if limit is not None:
        villages = villages[:limit]

    sem = asyncio.Semaphore(concurrency)
    seen_village_codes: set[str] = load_seen_village_codes(output_path)
    if verbose and seen_village_codes:
        print(f"RESUME enabled: {len(seen_village_codes)} villages already processed")

    tasks = []
    for v in villages:
        if v.village_code in seen_village_codes:
            if verbose:
                print(f"SKIP (resume) {v.village} ({v.village_code})")
            continue

        tasks.append(process_village(sem, v, verbose))

    results = await asyncio.gather(*tasks)

    records: list[AugmentedPostalCode] = []
    for r in results:
        if r and r.village_code not in seen_village_codes:
            records.append(r)
            seen_village_codes.add(r.village_code)

    write_jsonl(output_path, records)
    print(f"Done. Emitted {len(records)} records → {output_path}")


def main():
    parser = argparse.ArgumentParser(
        prog="postal-code-id-ingester",
        description="Postal code ingestion pipeline for Indonesia",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="Run ingestion")
    run.add_argument("--regions", required=True, help="regions_id.csv path")
    run.add_argument("--output", required=True, help="Output JSONL file")
    run.add_argument("--limit", type=int, help="Limit number of villages")
    run.add_argument("--verbose", action="store_true")
    run.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Max concurrent HTTP requests (default: 3)",
    )


    args = parser.parse_args()

    if args.command == "run":
        asyncio.run(
            run_ingestion(
                regions_path=args.regions,
                output_path=args.output,
                limit=args.limit,
                verbose=args.verbose,
                concurrency=args.concurrency,
            )
        )


if __name__ == "__main__":
    main()
