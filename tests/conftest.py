"""Shared pytest fixtures."""
import os
import shutil
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-marketing-data-platform")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def tmp_data_dir(tmp_path, monkeypatch):
    """Redirect all pipeline data paths to a tmp dir for isolation.

    Pipeline modules do `from pipelines.config import BRONZE` (direct binding),
    so monkeypatching config alone isn't enough — we also patch each module's
    local name so every read/write hits the same tmp dir.
    """
    import pipelines.config as cfg
    import pipelines.ingest as ingest_mod
    import pipelines.transform as transform_mod
    import pipelines.segment as segment_mod
    import pipelines.quality as quality_mod

    for attr, subdir in [("BRONZE", "bronze"), ("SILVER", "silver"), ("GOLD", "gold")]:
        new_path = tmp_path / subdir
        new_path.mkdir()
        for mod in (cfg, ingest_mod, transform_mod, segment_mod, quality_mod):
            if hasattr(mod, attr):
                monkeypatch.setattr(mod, attr, new_path)

    return tmp_path
