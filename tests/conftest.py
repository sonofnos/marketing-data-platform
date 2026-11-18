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
    """Redirect all pipeline data paths to a tmp dir for isolation."""
    import pipelines.config as cfg
    monkeypatch.setattr(cfg, "BRONZE", tmp_path / "bronze")
    monkeypatch.setattr(cfg, "SILVER", tmp_path / "silver")
    monkeypatch.setattr(cfg, "GOLD", tmp_path / "gold")
    (tmp_path / "bronze").mkdir()
    (tmp_path / "silver").mkdir()
    (tmp_path / "gold").mkdir()
    return tmp_path
