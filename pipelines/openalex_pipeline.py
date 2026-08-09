"""Lakeflow Declarative Pipeline: bronze -> silver -> gold over the harvested
OpenAlex corpus.

Source of works is harvester/snowball.py's landed JSON, not a Parquet
snapshot partition -- a partition slice's induced citation subgraph measures
at 0.09-0.18% density (see the plan / root CLAUDE.md), so the corpus is built
by topic-seeded citation snowball instead. The one deliberate exception is
`topics`, landed from the real snapshot by harvester/land_topics.py (small
enough, ~4,500 rows, to pull whole) -- this pipeline's genuine bulk-Parquet
touchpoint.

`spark` and the `dp`/`pipelines` decorators are injected by the Lakeflow
Declarative Pipelines runtime; this file is never run directly with `python`.
Cross-table reads inside the pipeline use plain `spark.read.table(...)` /
`spark.readStream.table(...)` -- confirmed against current Databricks docs,
not the classic `dlt.read()`/`dlt.read_stream()` some training data would
suggest; that spelling does not exist in the `pyspark.pipelines` module.

_reconstruct_abstract below is a deliberate duplicate of the root
abstract_reconstruction.py, not an import of it. A `sys.path.insert` +
`from abstract_reconstruction import ...` makes the function importable on
the driver, but F.udf() ships the wrapped function to executors via
cloudpickle, which pickles an importably-named function *by reference* --
each executor would need to `import abstract_reconstruction` itself, and
nothing distributes that file there. Defining the function directly in this
module sidesteps that: cloudpickle serializes it by value instead. Same
tradeoff already accepted for app/ and mcp_server/'s copied lakebase.py --
see CLAUDE.md's "Conventions" section -- keep this in sync with
abstract_reconstruction.py by hand; scripts/check_api.py tests the canonical
root copy, not this one.
"""

from __future__ import annotations

import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _reconstruct_abstract(inverted_index):  # noqa: ANN001, ANN201
    if inverted_index is None:
        return None
    if isinstance(inverted_index, str):
        stripped = inverted_index.strip()
        if not stripped:
            return None
        try:
            inverted_index = json.loads(stripped)
        except (json.JSONDecodeError, ValueError):
            return None
    if not isinstance(inverted_index, dict) or not inverted_index:
        return None
    position_to_word = {}
    for word, positions in inverted_index.items():
        if not isinstance(positions, (list, tuple)):
            continue
        for position in positions:
            if isinstance(position, int) and position >= 0:
                position_to_word[position] = word
    if not position_to_word:
        return None
    ordered = [position_to_word[p] for p in sorted(position_to_word)]
    text = " ".join(ordered).strip()
    return text or None


reconstruct_abstract_udf = F.udf(_reconstruct_abstract, T.StringType())

HARVEST_PATH = spark.conf.get("openalex.harvest_path", "/Volumes/main/research_copilot/raw/harvest")  # noqa: F821
TOPICS_DIR = spark.conf.get("openalex.topics_dir", "/Volumes/main/research_copilot/raw/topics")  # noqa: F821
SCHEMA_LOCATION = spark.conf.get("openalex.schema_location", "/Volumes/main/research_copilot/raw/_schemas")  # noqa: F821


def _short_id(col):
    """Spark-native "https://openalex.org/W123" -> "W123". Kept as a plain
    expression, not a Python UDF, for a value on the hot path (every work,
    every reference) -- harvester.snowball.short_id does the equivalent for
    the (comparatively rare) harvester-side Python calls.
    """
    return F.element_at(F.split(col, "/"), -1)


# ---------------------------------------------------------------------------
# Bronze -- raw, append-only, no reshaping
# ---------------------------------------------------------------------------


@dp.table(
    name="bronze_works_raw",
    comment=(
        "Raw OpenAlex work records from the topic-seeded citation snowball "
        "harvest (harvester/snowball.py). abstract_inverted_index is pinned "
        "to STRING via a schema hint -- its keys are the abstract's own "
        "words, an unbounded vocabulary, so letting Auto Loader infer a "
        "struct from it would try to build one field per distinct word."
    ),
)
def bronze_works_raw():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION}/works")
        .option("cloudFiles.schemaHints", "abstract_inverted_index STRING")
        .load(HARVEST_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit("api_snowball"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze_dim_topics",
    comment=(
        "OpenAlex's full topic taxonomy (~4,500 rows), landed from the "
        "Parquet snapshot by harvester/land_topics.py -- the one place this "
        "pipeline reads the snapshot directly rather than the API."
    ),
)
def bronze_dim_topics():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION}/topics")
        .load(TOPICS_DIR)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit("snapshot_dim"))
    )


# ---------------------------------------------------------------------------
# Silver -- explicit schema contract + quality gates
# ---------------------------------------------------------------------------

_SILVER_WORKS_GATES = {
    "valid_id": "work_id IS NOT NULL",
    "has_abstract": "narrative_abstract IS NOT NULL AND length(narrative_abstract) > 100",
    "plausible_year": "publication_year IS NULL OR publication_year BETWEEN 1800 AND year(current_date()) + 1",
}


@dp.materialized_view(
    name="silver_works",
    comment=(
        "One row per work: work_id normalized to the bare id, abstract "
        "reconstructed from the inverted index, topic/OA fields flattened. "
        "Explicit column list below is the schema contract -- not inferred."
    ),
)
@dp.expect_all_or_drop(_SILVER_WORKS_GATES)
@dp.expect(
    "has_citations",
    "size(referenced_works) > 0",
)  # warn, don't drop -- a work with zero recorded references is real OpenAlex
# data (confirmed during corpus planning: ~42% of works have none), not
# necessarily a bad row.
@dp.expect_or_fail("no_duplicate_work_ids", "count(*) OVER (PARTITION BY work_id) = 1")
def silver_works():
    raw = spark.read.table("bronze_works_raw")  # noqa: F821
    return (
        raw.withColumn("work_id", _short_id(F.col("id")))
        .withColumn(
            "referenced_works",
            F.transform(F.coalesce(F.col("referenced_works"), F.array()), lambda x: _short_id(x)),
        )
        .select(
            F.col("work_id"),
            F.col("display_name").alias("title"),
            F.col("doi"),
            F.col("publication_year"),
            F.col("primary_topic.domain.display_name").alias("primary_topic_domain"),
            F.col("primary_topic.field.display_name").alias("primary_topic_field"),
            F.coalesce(F.col("cited_by_count"), F.lit(0)).alias("cited_by_count"),
            F.col("fwci"),
            F.coalesce(F.col("open_access.is_oa"), F.lit(False)).alias("is_oa"),
            F.col("open_access.oa_url").alias("oa_url"),
            F.col("language"),
            F.col("_seed_topic").alias("seed_topic"),
            F.col("_hop"),
            reconstruct_abstract_udf(F.col("abstract_inverted_index")).alias("narrative_abstract"),
            F.col("referenced_works"),
            F.col("authorships"),
            F.to_json(
                F.struct(F.col("id"), F.col("display_name"), F.col("primary_topic"), F.col("open_access"))
            ).alias("payload"),
            F.col("_ingested_at"),
            F.col("_source"),
        )
    )


@dp.materialized_view(
    name="silver_authors",
    comment="Authors deduped from every harvested work's own authorships field -- no separate fetch needed.",
)
def silver_authors():
    exploded = spark.read.table("silver_works").select(F.explode(F.col("authorships")).alias("a"))  # noqa: F821
    return (
        exploded.select(
            _short_id(F.col("a.author.id")).alias("author_id"),
            F.col("a.author.display_name").alias("display_name"),
            F.col("a.author.orcid").alias("orcid"),
        )
        .where(F.col("author_id").isNotNull())
        .dropDuplicates(["author_id"])
    )


@dp.materialized_view(
    name="silver_paper_authors",
    comment="work_id/author_id bridge, exploded from authorships, position-ordered.",
)
def silver_paper_authors():
    exploded = spark.read.table("silver_works").select(  # noqa: F821
        F.col("work_id"), F.posexplode(F.col("authorships")).alias("author_position", "a")
    )
    return exploded.select(
        F.col("work_id"),
        _short_id(F.col("a.author.id")).alias("author_id"),
        F.col("author_position"),
        F.coalesce(F.col("a.is_corresponding"), F.lit(False)).alias("is_corresponding"),
    ).where(F.col("author_id").isNotNull())


@dp.materialized_view(
    name="silver_citation_edges",
    comment=(
        "Raw citing->cited pairs, exploded from referenced_works. NOT yet "
        "restricted to the induced subgraph -- gold_citation_graph does that."
    ),
)
def silver_citation_edges():
    return spark.read.table("silver_works").select(  # noqa: F821
        F.col("work_id").alias("citing_work_id"), F.explode(F.col("referenced_works")).alias("cited_work_id")
    )


@dp.materialized_view(
    name="silver_topics_dim",
    comment="OpenAlex's full topic taxonomy, flattened from the landed snapshot entity.",
)
def silver_topics_dim():
    return spark.read.table("bronze_dim_topics").select(  # noqa: F821
        _short_id(F.col("id")).alias("topic_id"),
        F.col("display_name"),
        F.col("domain.display_name").alias("domain"),
        F.col("field.display_name").alias("field"),
        F.col("subfield.display_name").alias("subfield"),
        F.col("description"),
        F.col("keywords"),
    )


# ---------------------------------------------------------------------------
# Gold -- the differentiator
# ---------------------------------------------------------------------------


@dp.materialized_view(
    name="gold_citation_graph",
    comment=(
        "Citation edges restricted to the induced subgraph -- both endpoints "
        "must be in silver_works. This is what build_reading_path() "
        "(reading_path.py) consumes; mirrored into Lakebase's citation_edges "
        "table for serving so the UI never waits on a Spark round trip."
    ),
)
def gold_citation_graph():
    edges = spark.read.table("silver_citation_edges")  # noqa: F821
    work_ids = spark.read.table("silver_works").select("work_id")  # noqa: F821
    return (
        edges.join(work_ids.withColumnRenamed("work_id", "citing_work_id"), "citing_work_id")
        .join(work_ids.withColumnRenamed("work_id", "cited_work_id"), "cited_work_id")
        .dropDuplicates(["citing_work_id", "cited_work_id"])
    )


@dp.materialized_view(
    name="gold_paper_metrics",
    comment=(
        "foundational_score blends in-corpus in-degree (the most direct "
        "'foundational within this reading set' signal, weighted highest), "
        "fwci (already field- and age-normalized, so it doesn't just reward "
        "old papers), and raw cited_by_count as a tiebreaker."
    ),
)
def gold_paper_metrics():
    works = spark.read.table("silver_works")  # noqa: F821
    in_degree = (
        spark.read.table("gold_citation_graph")  # noqa: F821
        .groupBy(F.col("cited_work_id").alias("work_id"))
        .agg(F.count("*").alias("in_corpus_in_degree"))
    )
    return (
        works.join(in_degree, "work_id", "left")
        .withColumn("in_corpus_in_degree", F.coalesce(F.col("in_corpus_in_degree"), F.lit(0)))
        .withColumn(
            "foundational_score",
            3 * F.log1p(F.col("in_corpus_in_degree"))
            + F.log1p(F.col("cited_by_count"))
            + F.coalesce(F.col("fwci"), F.lit(0.0)),
        )
        .select("work_id", "in_corpus_in_degree", "cited_by_count", "fwci", "publication_year", "foundational_score")
    )


@dp.materialized_view(
    name="gold_topic_rollups",
    comment="Paper/citation counts per seed topic -- powers the app's browse/filter view.",
)
def gold_topic_rollups():
    return (
        spark.read.table("silver_works")  # noqa: F821
        .groupBy("seed_topic")
        .agg(F.count("*").alias("paper_count"), F.sum("cited_by_count").alias("total_citations"))
    )


@dp.materialized_view(
    name="gold_author_metrics",
    comment="Paper count and total citations per author, from the harvested corpus only (not OpenAlex's global stats).",
)
def gold_author_metrics():
    paper_authors = spark.read.table("silver_paper_authors")  # noqa: F821
    works = spark.read.table("silver_works").select("work_id", "cited_by_count")  # noqa: F821
    return (
        paper_authors.join(works, "work_id")
        .groupBy("author_id")
        .agg(F.count("*").alias("paper_count"), F.sum("cited_by_count").alias("total_citations"))
    )


@dp.materialized_view(
    name="gold_papers_for_serving",
    comment="The curated subset pushed to Lakebase -- matches sql/01_schema.sql's papers table exactly.",
)
def gold_papers_for_serving():
    works = spark.read.table("silver_works")  # noqa: F821
    metrics = spark.read.table("gold_paper_metrics").select("work_id", "foundational_score")  # noqa: F821
    return works.join(metrics, "work_id").select(
        "work_id",
        "title",
        "doi",
        "publication_year",
        "primary_topic_domain",
        "primary_topic_field",
        "cited_by_count",
        "fwci",
        "foundational_score",
        "is_oa",
        "oa_url",
        "language",
        "seed_topic",
        "narrative_abstract",
        "payload",
    )
