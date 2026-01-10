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


async def run_ingestion(
    regions_path: str,
    output_path: str,
    limit: int | None = None,
    verbose: bool = False,
):
    villages = load_villages_from_region_id(regions_path)

    if limit is not None:
        villages = villages[:limit]

    records: list[AugmentedPostalCode] = []
    seen_village_codes: set[str] = set()

    for idx, v in enumerate(villages, start=1):
        if v.village_code in seen_village_codes:
            if verbose:
                print(f"SKIP duplicate village_code {v.village_code}")
            continue

        if verbose:
            print(f"[{idx}/{len(villages)}] {v.village} ({v.village_code})")

        try:
            html = await fetch_postal_html(v.village)
        except Exception as e:
            if verbose:
                print("  FETCH ERROR:", e)
            continue

        candidates = parse_postal_results(html)
        matched = False

        for c in candidates:
            score = match_postal_candidate(v, c)
            if score:
                records.append(
                    AugmentedPostalCode(
                        village_code=v.village_code,
                        postal_code=c["postal_code"],
                        source="pos-indonesia",
                        confidence=round(score, 3),
                        retrieved_at=AugmentedPostalCode.now_iso(),
                        raw=c,
                    )
                )
                seen_village_codes.add(v.village_code)
                matched = True
                break

        if verbose and not matched:
            print(f"  NO MATCH for {v.village}")

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

    args = parser.parse_args()

    if args.command == "run":
        asyncio.run(
            run_ingestion(
                regions_path=args.regions,
                output_path=args.output,
                limit=args.limit,
                verbose=args.verbose,
            )
        )


if __name__ == "__main__":
    main()
