"""Entry point: run the full pipeline end-to-end."""
import sys

from pipelines import ingest, quality, segment, transform
from pipelines.config import get_spark


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== Bronze: Ingestion ===")
    ingest.run_all(spark)

    print("\n=== Silver + Gold: Transformations ===")
    transform.run_all(spark)

    print("\n=== Gold: Segmentation ===")
    segment.run_all(spark)

    print("\n=== Quality checks ===")
    raise_on_fail = "--strict" in sys.argv
    quality.run_all(spark, raise_on_failure=raise_on_fail)

    print("\nPipeline complete.")
    spark.stop()


if __name__ == "__main__":
    main()
