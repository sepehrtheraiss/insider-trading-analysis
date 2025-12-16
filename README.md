# Insider Trading Analysis

Build a **local, queryable dataset of SEC insider trading filings** (Forms **3**, **4**, and **5**), enrich it with market metadata, and analyze/visualize insider behavior.

This project is **not** about prediction or trade automation. It’s **data engineering + exploratory analysis** of legally disclosed insider transactions.

---

## What This Project Does

At a high level, the project:

1. Fetches insider trading filings from the SEC via an external API
2. Normalizes and cleans raw filing data
3. Enriches transactions with issuer metadata (exchange, sector, industry)
4. Stores data in a PostgreSQL database
5. Aggregates and visualizes insider activity (buy/sell value, reporters, sectors, time)

---

## Docs

Setup, design, environment variables, DB access, and more:

➡️ **[`docs/README.md`](docs/README.md)**

---


## Examples

### Top insider activity by reporter

```bash
insider_cli plot n_most_companies_bs_by_reporter   --start "2022-01-01"   --end "2022-12-31"   --show
```
![](assets/top_15_by_reporter_2022-01-01_2022-12-31.png)




---

### Deep dive TSLA trades

```bash
insider_cli plot acquired_disposed_line_chart_ticker   --ticker TSLA   --start "2022-01-01"   --end "2022-12-31"   --show
```
![](assets/TSLA_line_chart_2022-01-01_2022-12-31.png)

---

### Zoom in Elon Musk trades

```bash
insider_cli plot acquired_disposed_line_chart_ticker   --ticker TSLA   --reporter "Elon Musk"   --start "2022-01-01"   --end "2022-12-31"   --show
```
![](assets/TSLA_line_chart_Elon_2022-01-01_2022-12-31.png)

---

### Other insiders trading TSLA

```bash
insider_cli plot n_most_companies_bs_by_reporter   --ticker TSLA   --start "2022-01-01"   --end "2022-12-31"   --show
```
![](assets/top_15_by_reporter_TSLA_2022-01-01_2022-12-31.png)

### Sector Stats

```bash
insider_cli plot sector_statistics --start "2019-01-01" --end "2025-10-30" --show
```
![](assets/sector_stats_2019-01-01_2025-10-30.png)
---
