# postal-code-id-ingester

Python-based ingestion pipeline to augment Indonesian postal code data
using **region-id village dataset**.

## What this repo is

- Scrapes postal code sources (e.g. Pos Indonesia)
- Matches results against province / district / village
- Emits structured augmentation data (JSON)

## What this repo is NOT

- Not a dataset
- Not an API
- Not a source of truth

## Input

Village-level data from `region-id`.

## Output

```json
{
  "village_code": "3273051001",
  "postal_code": "40115",
  "source": "pos-indonesia",
  "confidence": 0.8
}
```
