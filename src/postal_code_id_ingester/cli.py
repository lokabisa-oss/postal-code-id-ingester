import argparse
import asyncio

from postal_code_id_ingester.ingest.region_id_loader import (
    load_villages_from_region_id
)

from postal_code_id_ingester.ingest.failed_loader import (
    load_failed_villages
)
from postal_code_id_ingester.ingest.fetcher import fetch_postal_html
from postal_code_id_ingester.sources.pos_indonesia import parse_postal_results
from postal_code_id_ingester.matchers.region_matcher import (
    match_postal_candidate,
    match_postal_candidate_override
)
from postal_code_id_ingester.model.augmented import AugmentedPostalCode
from postal_code_id_ingester.export.jsonl import write_jsonl
from postal_code_id_ingester.export.resume import load_seen_village_codes
from postal_code_id_ingester.query.keywords import (
    extract_single_word,
    extract_prefix_keywords,
    normalize_city_name,
)
from postal_code_id_ingester.ingest.override_loader import load_override_rules



async def process_village(
    sem: asyncio.Semaphore,
    v,
    override_rules: dict,
    enable_overrides: bool,
    verbose: bool = False,
):
    async with sem:
        if verbose:
            print(f"PROCESS {v.village} ({v.village_code})")

        # Keyword strategy (ORDER MATTERS)
        raw_keywords = [
            v.village,                     # 1. default (as-is)
            v.district,                    # 2. fallback district
        ]

        # 3. progressive village prefix
        raw_keywords.extend(extract_prefix_keywords(v.village))

        # 4. progressive district prefix (optional, safer belakangan)
        raw_keywords.extend(extract_prefix_keywords(v.district))

        # 5. single word LAST fallback
        raw_keywords.append(extract_single_word(v.village))

        # 6) city-level LAST RESORT
        city_keyword = normalize_city_name(v.city)
        if city_keyword:
            raw_keywords.append(city_keyword)

        # ---- normalize & dedup ----
        seen = set()
        keywords = []
        for k in raw_keywords:
            if not k:
                continue

            k = k.strip()
            if len(k) < 3:
                continue

            key = k.lower()
            if key in seen:
                continue

            seen.add(key)
            keywords.append(k)

        if verbose:
            print(f"  KEYWORDS ({len(keywords)}): {keywords}")

        for keyword in keywords:
            is_city_level = (keyword == city_keyword)

            try:
                html = await fetch_postal_html(keyword)
            except Exception as e:
                if verbose:
                    print(f"    FETCH ERROR keyword={keyword}: {e}")
                continue

            candidates = parse_postal_results(html)
            if not candidates:
                continue

            for c in candidates:
                score = match_postal_candidate(
                    v, 
                    c,
                    mode="city" if is_city_level else "village",
                )
                if score:
                    if verbose:
                        print(
                            f"    MATCH keyword='{keyword}' "
                            f"postal_code={c['postal_code']} "
                            f"score={score}"
                        )
                    return AugmentedPostalCode(
                        village_code=v.village_code,
                        postal_code=c["postal_code"],
                        source="pos-indonesia",
                        confidence=score,
                        retrieved_at=AugmentedPostalCode.now_iso(),
                        raw=c,
                    )

        # ---------- PHASE 2: OVERRIDE (LAST RESORT) ----------
        if enable_overrides:
            if verbose:
                print(f"  OVERRIDE HIT for {v.village_code}")

            rule = None

            # village-level override
            rule = override_rules.get(("village", v.village_code))

            # district-level override (fallback)
            if not rule and hasattr(v, "district_code"):
                rule = override_rules.get(("district", v.district_code))

            if rule:
                if verbose:
                    print(
                        f"  OVERRIDE keyword='{rule.postal_alias}' "
                        f"mode={rule.match_mode}"
                    )

                try:
                    html = await fetch_postal_html(rule.postal_alias)
                except Exception as e:
                    if verbose:
                        print(f"    OVERRIDE FETCH ERROR: {e}")
                    return None

                candidates = parse_postal_results(html)

                for c in candidates:
                    score = match_postal_candidate_override(
                        v,
                        c,
                        mode=rule.match_mode,
                        postal_alias=rule.postal_alias,
                    )
                    if score:
                        if verbose:
                            print(
                                f"    OVERRIDE MATCH "
                                f"postal_code={c['postal_code']} "
                                f"score={score}"
                            )
                        return AugmentedPostalCode(
                            village_code=v.village_code,
                            postal_code=c["postal_code"],
                            source="pos-indonesia-override",
                            confidence=score,
                            retrieved_at=AugmentedPostalCode.now_iso(),
                            raw=c,
                        )

        if verbose:
            print(f"  NO MATCH {v.village}")

        return None


async def run_ingestion(
    regions_path: str,
    output_path: str,
    override_path: str | None = None,
    enable_overrides: bool = False,
    limit: int | None = None,
    verbose: bool = False,
    concurrency: int = 3,
):
    if regions_path.endswith("failed_regions.csv"):
        villages = load_failed_villages(regions_path)
    else:
        villages = load_villages_from_region_id(regions_path)

    if limit is not None:
        villages = villages[:limit]

    sem = asyncio.Semaphore(concurrency)

    use_resume = not regions_path.endswith("failed_regions.csv")

    seen_village_codes: set[str] = (
        load_seen_village_codes(output_path) if use_resume else set()
    )

    if verbose and use_resume and seen_village_codes:
        print(f"RESUME enabled: {len(seen_village_codes)} villages already processed")

    if verbose and not use_resume:
        print("RESUME disabled (failed-only mode)")

    override_rules = {}
    if enable_overrides and override_path:
        override_rules = load_override_rules(override_path)
        if verbose:
            print(f"OVERRIDES loaded: {len(override_rules)} rules")

    tasks = []
    for v in villages:
        if v.village_code in seen_village_codes:
            if verbose:
                print(f"SKIP (resume) {v.village} ({v.village_code})")
            continue

        tasks.append(
            process_village(
                sem,
                v,
                override_rules,
                enable_overrides,
                verbose,
            )
        )

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
    run.add_argument(
        "--enable-overrides",
        action="store_true",
        help="Enable postal name override as last resort",
    )

    run.add_argument(
        "--override-table",
        help="CSV file containing postal override rules",
    )


    args = parser.parse_args()

    if args.command == "run":
        asyncio.run(
            run_ingestion(
                regions_path=args.regions,
                output_path=args.output,
                override_path=args.override_table,
                enable_overrides=args.enable_overrides,
                limit=args.limit,
                verbose=args.verbose,
                concurrency=args.concurrency,
            )
        )


if __name__ == "__main__":
    main()
