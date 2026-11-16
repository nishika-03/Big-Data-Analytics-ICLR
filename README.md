# Big Data Analytics: ICLR Peer Review and Citation Analysis

PySpark analysis (run on Databricks) of ICLR peer-review data joined with OpenAlex citation metadata. It covers review outcomes, reviewer scores, emerging research keywords, and whether reviewer scores predict citation impact.

## Repository structure

| Path | Contents |
|---|---|
| `notebooks/Big_Data_Analytics_ICLR.ipynb` | Main PySpark notebook (Databricks) |
| `notebooks/Big_Data_Analytics_ICLR.html` | Rendered HTML export with outputs |
| `report/Big_Data_Analytics_ICLR_Report.pdf` | Written report |
| `data/iclr26v1.parquet` | ICLR submissions: reviewer scores, decisions and keywords |
| `data/openalex.json` | OpenAlex metadata used for the join |

## Analysis questions and extensions

1. **Accepted / rejected / withdrawn counts.** Extension: z-score analysis of annual acceptance rates to find anomalous years.
2. **Accepted papers with at least one low score (< 5).** Extension: min–max normalisation of mean scores to grade reviewer disagreement.
3. **Top 10 papers by mean score.** Extension: score variance analysis (consensus vs outlier inflation).
4. **Rapidly increasing keywords.** Extension: full growth trajectory and year-on-year growth rates.
5. **ICLR papers matched to OpenAlex.** Extension: effect of title normalisation on the number of matches.
6. **Low-score but highly cited papers.** Extension: Pearson correlation and score-band analysis of reviewer score vs citation impact.

## Tech stack

PySpark (DataFrames, `pyspark.sql.functions`, window functions) and Databricks (Unity Catalog volumes).

## Running it

1. Upload `data/iclr26v1.parquet` and `data/openalex.json` to a Databricks volume.
2. Update `ICLR_PATH` and `OPENALEX_PATH` in the first code cell.
3. Run all cells.
