"""Big Data Analytics: ICLR peer review and citation analysis (PySpark on Databricks).

Plain-Python export of notebooks/Big_Data_Analytics_ICLR.ipynb.
Run inside Databricks, where `spark`, `display` and `dbutils` are provided.
"""



# # Environment Setup and Data Loading
#
# - This notebook uses PySpark in Databricks to analyse the ICLR and OpenAlex datasets.
#
# - Both datasets are loaded using Spark DataFrames from the required file paths. 
# Initial checks such as `printSchema(), count(), and display()` are used to understand 
# the structure of the data, including column types, nested fields, and overall size.
#
# - This follows the approach used in the lab practicals, where data is explored first 
# before applying transformations. All processing is done using PySpark functions, 
# ensuring the solution meets the coursework requirement of using distributed 
# (big data) tools rather than non-parallel approaches like pandas.


from pyspark.sql import functions as F
from pyspark.sql.window import Window


ICLR_PATH = "/Volumes/workspace/default/big_data/iclr26v1.parquet"
OPENALEX_PATH = "/Volumes/workspace/default/big_data/openalex.json"

df_iclr = spark.read.parquet(ICLR_PATH)
df_openalex = spark.read.json(OPENALEX_PATH)


display(dbutils.fs.ls("/Volumes/workspace/default/big_data/"))


df_iclr.printSchema()
df_openalex.printSchema()


print("ICLR rows:", df_iclr.count())
print("OpenAlex rows:", df_openalex.count())


display(df_iclr.limit(5))
display(df_openalex.limit(5))


# # Data Cleaning and Preparation
#
# Only the preprocessing steps required for the analysis are applied to the datasets.
#
# - First, the decision column is standardised into consistent categories (accepted, rejected, and withdrawn), as the original data contains different formats for similar outcomes. This is necessary for Questions 1 and 2.
#
# - Next, a mean score is calculated from the reviewer scores array. This is required for Questions 3 and 6, where numerical comparisons of paper quality are needed.
#
# - Finally, paper titles are cleaned (lowercased and stripped of punctuation) to support dataset matching in Questions 5 and 6.
#
# - 
# No additional cleaning steps are applied to the basic question answers. The `title_clean `column is used only for extension analyses, ensuring the basic Q5 join remains consistent with the provided model answers as required by the assignment brief.


df_iclr_clean = (
    df_iclr
    .withColumn("decision_lower", F.lower(F.trim(F.col("decision"))))
    .withColumn(
        "decision_clean",
        F.when(F.col("decision_lower").contains("withdraw"), "withdrawn")
        .when(
            (F.col("decision_lower").contains("accept")) |
            (F.col("decision_lower").contains("poster")) |
            (F.col("decision_lower").contains("oral")) |
            (F.col("decision_lower").contains("spotlight")) |
            (F.col("decision_lower").contains("workshop")),
            "accepted"
        )
        .when(F.col("decision_lower").contains("reject"), "rejected")
        .otherwise("other")
    )
    .withColumn(
        "mean_score",
        F.when(
            F.size("scores") > 0,
            F.expr("aggregate(scores, 0D, (acc, x) -> acc + x) / size(scores)")
        )
    )
    .withColumn(
        "title_clean",
        F.lower(F.trim(F.regexp_replace(F.col("title"), "[^a-zA-Z0-9 ]", " ")))
    )
)

df_openalex_clean = df_openalex.withColumn(
    "title_clean",
    F.lower(F.trim(F.regexp_replace(F.col("title"), "[^a-zA-Z0-9 ]", " ")))
)

display(df_iclr_clean.limit(5))


display(
    df_iclr_clean
    .groupBy("decision", "decision_clean")
    .count()
)


display(
    df_iclr_clean
    .select("scores", "mean_score")
)


# # Question 1: Number of Accepted, Rejected and Withdrawn Papers


q1_result = (
    df_iclr_clean
    .filter(F.col("decision_clean").isin("accepted", "rejected", "withdrawn",))
    .groupBy("decision_clean")
    .agg(F.count("*").alias("paper_count"))
    .orderBy("decision_clean")
)

display(q1_result)


# # 1.1 Extension: Z-Score Analysis of Annual Acceptance Rates — Identifying Statistically Anomalous Years


# Compute acceptance rate per year
acceptance_by_year = (
    df_iclr_clean
    .filter(F.col("decision_clean").isin("accepted", "rejected", "withdrawn"))
    .groupBy("year")
    .agg(
        F.count("*").alias("total_submissions"),
        F.sum(
            F.when(F.col("decision_clean") == "accepted", 1).otherwise(0)
        ).alias("accepted_count")
    )
    .withColumn(
        "acceptance_rate",
        F.round(F.col("accepted_count") / F.col("total_submissions") * 100, 2)
    )
)

# Z-Score: how many standard deviations is each year from the mean acceptance rate
window_all = Window.rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing
)

q1_extension_zscore = (
    acceptance_by_year
    .withColumn("mean_rate", F.avg("acceptance_rate").over(window_all))
    .withColumn("std_rate", F.stddev("acceptance_rate").over(window_all))
    .withColumn(
        "z_score",
        F.round(
            (F.col("acceptance_rate") - F.col("mean_rate")) / F.col("std_rate"),
            3
        )
    )
    .withColumn(
        "year_type",
        F.when(F.col("z_score") >  1.5, "Unusually Lenient")
         .when(F.col("z_score") < -1.5, "Unusually Selective")
         .otherwise("Normal Range")
    )
    .select(
        "year", "total_submissions", "accepted_count",
        "acceptance_rate", "z_score", "year_type"
    )
    .orderBy("year")
)

display(q1_extension_zscore)


# # Question 2: Accepted Papers with At Least One Low Score


q2_result = (
    df_iclr_clean
    .filter(
        (F.col("decision_clean") == "accepted") &
        (F.array_min(F.col("scores")) < 5)
    )
)

q2_count = q2_result.count()
print("Number of accepted papers with at least one score below 5:", q2_count)


# # 2.1 Extension: Min-Max Normalisation of Mean Scores — Classifying Low-Score Accepted Papers by Severity of Reviewer Disagreement


# Min-Max Normalisation of mean scores for low-score accepted papers

# Step 1: Get global min and max
bounds = (
    df_iclr_clean
    .filter(
        (F.col("decision_clean") == "accepted") &
        (F.array_min(F.col("scores")) < 5) &
        F.col("mean_score").isNotNull()
    )
    .agg(
        F.min("mean_score").alias("min_score"),
        F.max("mean_score").alias("max_score")
    )
    .collect()[0]
)

min_s = bounds["min_score"]
max_s = bounds["max_score"]

# Step 2: Apply formula: normalised = (x - min) / (max - min)
q2_extension = (
    df_iclr_clean
    .filter(
        (F.col("decision_clean") == "accepted") &
        (F.array_min(F.col("scores")) < 5) &
        F.col("mean_score").isNotNull()
    )
    .withColumn(
        "normalised_score",
        F.round(
            (F.col("mean_score") - F.lit(min_s)) / F.lit(max_s - min_s),
            3
        )
    )
    .withColumn(
        "score_tier",
        F.when(F.col("normalised_score") >= 0.7, "Upper Tier")
         .when(F.col("normalised_score") >= 0.4, "Mid Tier")
         .otherwise("Lower Tier")
    )
    .groupBy("score_tier")
    .agg(
        F.count("*").alias("paper_count"),
        F.round(F.avg("mean_score"), 2).alias("avg_raw_score"),
        F.round(F.avg("normalised_score"), 3).alias("avg_normalised_score")
    )
    .orderBy(F.col("avg_normalised_score").desc())
)

display(q2_extension)


# # Question 3: Top 10 Papers with Highest Mean Score


q3_result = (
    df_iclr_clean
    .filter(F.col("mean_score").isNotNull())
    .select(
        "id",
        "mean_score",
        "year"
    )
    .orderBy(F.col("mean_score").desc())
    .limit(10)
)

display(q3_result)


# # 3.1 Extension: Score Variance Analysis — Verifying Whether Top 10 Mean Scores Reflect Genuine Reviewer Consensus or Outlier Inflation


# Score Variance (Standard Deviation) for Top 10 Papers

top10_ids = [row["id"] for row in q3_result.select("id").collect()]

q3_extension = (
    df_iclr_clean
    .filter(F.col("id").isin(top10_ids))
    .select(
        "id", "year", "mean_score", "decision_clean",
        F.explode("scores").alias("score")
    )
    .groupBy("id", "year", "mean_score", "decision_clean")
    .agg(
        F.round(F.stddev("score"), 3).alias("score_stddev"),
        F.count("score").alias("num_reviewers")
    )
    .orderBy(F.col("mean_score").desc())
)

display(q3_extension)


# # Question 4: Rapidly Increasing Keywords


keywords_df = (
    df_iclr_clean
    .select("year", F.explode("keywords").alias("keyword"))
    .withColumn("keyword_clean", F.lower(F.trim(F.col("keyword"))))
    .filter(F.col("keyword_clean").isNotNull())
    .filter(F.col("keyword_clean") != "")
)

keyword_counts = (
    keywords_df
    .groupBy("keyword_clean", "year")
    .agg(F.count("*").alias("keyword_count"))
)

keyword_growth = (
    keyword_counts.alias("current")
    .join(
        keyword_counts.alias("next"),
        (F.col("current.keyword_clean") == F.col("next.keyword_clean")) &
        (F.col("next.year") == F.col("current.year") + 1),
        "inner"
    )
    .select(
        F.col("current.keyword_clean").alias("keyword"),
        F.col("current.year").alias("year"),
        F.col("current.keyword_count").alias("low_count"),
        F.col("next.keyword_count").alias("high_count"),
        F.col("next.year").alias("following_year")
    )
)

q4_result = (
    keyword_growth
    .filter((F.col("low_count") < 5) & (F.col("high_count") > 50))
    .orderBy(F.col("high_count").desc())
    .limit(10)
)

display(q4_result)


# # 4.1 Extension: Full Growth Trajectory and Year-on-Year Growth Rate Analysis of Identified Emerging Keywords


# Get identified keywords from Q4 basic result
q4_extension_keywords = [
    row["keyword"] for row in q4_result.select("keyword").collect()
]

# Self-contained: recompute clean keyword counts directly
kw_clean = (
    df_iclr_clean
    .select("year", F.explode("keywords").alias("kw"))
    .withColumn("keyword_clean", F.lower(F.trim(F.col("kw"))))
    .filter(F.col("keyword_clean").isNotNull())
    .filter(F.col("keyword_clean") != "")
    .groupBy("keyword_clean", "year")
    .agg(F.count("*").alias("keyword_count"))
)

# Window lag: get previous year count for each keyword
window_kw = Window.partitionBy("keyword_clean").orderBy("year")

q4_extension = (
    kw_clean
    .filter(F.col("keyword_clean").isin(q4_extension_keywords))
    .withColumn(
        "prev_count",
        F.lag("keyword_count", 1).over(window_kw)
    )
    .withColumn(
        "growth_rate_pct",
        F.when(
            (F.col("prev_count").isNotNull()) & (F.col("prev_count") > 0),
            F.round(
                (F.col("keyword_count") - F.col("prev_count"))
                / F.col("prev_count") * 100, 1
            )
        ).otherwise(F.lit(None))
    )
    .select(
        "keyword_clean", "year",
        "keyword_count", "prev_count", "growth_rate_pct"
    )
    .orderBy("keyword_clean", "year")
)

display(q4_extension)


# # Question 5: ICLR Papers with OpenAlex Information


q5_join = (
    df_iclr.alias("iclr")
    .join(
        df_openalex.alias("oa"),
        F.col("iclr.title") == F.col("oa.title"),
        "inner"
    )
)

q5_result = (
    q5_join
    .groupBy(F.col("iclr.year").alias("year"))
    .agg(F.count("*").alias("matched_papers"))
    .orderBy("year")
)

display(q5_result)


# # 5.1 Extension: Impact of Title Normalisation on Match Count — Quantifying Additional Matches Recovered Through Cleaned Title Join


# Extension: Impact of Title Normalisation on Match Count

q5_cleaned_result = (
    df_iclr_clean.alias("iclr")
    .join(
        df_openalex_clean.alias("oa"),
        F.col("iclr.title_clean") == F.col("oa.title_clean"),
        "inner"
    )
    .groupBy(F.col("iclr.year").alias("year"))
    .agg(F.count("*").alias("cleaned_title_matches"))
)

q5_exact_result = (
    q5_result
    .withColumnRenamed("matched_papers", "exact_title_matches")
)

q5_extension = (
    q5_exact_result
    .join(q5_cleaned_result, "year", "outer")
    .withColumn(
        "additional_matches",
        F.col("cleaned_title_matches") - F.col("exact_title_matches")
    )
    .orderBy("year")
)

display(q5_extension)


# # Question 6: Low Score but Highly Cited Papers


q6_join = (
    df_iclr_clean.alias("iclr")
    .join(
        df_openalex.alias("oa"),
        F.col("iclr.title") == F.col("oa.title"),
        "inner"
    )
)


p95 = q6_join.approxQuantile("cited_by_count", [0.95], 0.01)[0]
print("95th percentile:", p95)


q6_result = (
    q6_join
    .filter(
        (F.col("iclr.mean_score") < 5) &
        (F.col("oa.citation_normalized_percentile.value") >= 0.95)
    )
    .select(
        F.col("iclr.id").alias("id"),
        F.col("iclr.year").alias("year"),
    )
    .orderBy("year", "id")
)
display(q6_result)


# # 6.1 Extension: Pearson Correlation and Score Band Analysis — Quantifying the Relationship Between Reviewer Scores and Long-Term Citation Impact


# Extension: Does reviewer score predict citation impact?

# Step 1: Clean deduplicated base
q6_base = (
    q6_join
    .filter(F.col("iclr.mean_score").isNotNull())
    .filter(F.col("oa.cited_by_count").isNotNull())
    .select(
        F.col("iclr.id").alias("id"),
        F.col("iclr.mean_score").alias("mean_score"),
        F.col("oa.cited_by_count").alias("cited_by_count")
    )
    .groupBy("id", "mean_score")
    .agg(F.max("cited_by_count").alias("cited_by_count"))
)

# Step 2: Pearson correlation
pearson = q6_base.agg(
    F.round(F.corr("mean_score", "cited_by_count"), 4)
    .alias("pearson_correlation")
)
display(pearson)

# Step 3: Score band analysis
q6_extension = (
    q6_base
    .withColumn("score_band",
        F.when(F.col("mean_score") < 4, "Low (below 4)")
         .when(F.col("mean_score") < 6, "Medium (4–6)")
         .when(F.col("mean_score") < 8, "High (6–8)")
         .otherwise("Very High (8+)")
    )
    .groupBy("score_band")
    .agg(
        F.count("*").alias("paper_count"),
        F.round(F.avg("cited_by_count"), 1).alias("avg_citations"),
        F.round(F.max("cited_by_count"), 0).alias("max_citations")
    )
    .orderBy("score_band")
)
display(q6_extension)
