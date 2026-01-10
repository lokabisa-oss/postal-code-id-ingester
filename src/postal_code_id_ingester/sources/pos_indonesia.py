from pyquery import PyQuery as pq


def parse_postal_results(html: str) -> list[dict]:
    doc = pq(html)
    results: list[dict] = []

    for row in doc("#list-data tbody tr").items():
        cols = [c.text().strip() for c in row("td").items()]

        if len(cols) < 6:
            continue

        results.append({
            "postal_code": cols[1],
            "village": cols[2],
            "district": cols[3],
            "city": cols[4],
            "province": cols[5],
        })

    return results
