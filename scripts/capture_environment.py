# Databricks notebook source

import json, os, platform, sys, importlib.metadata as md
from datetime import datetime, timezone
from pathlib import Path

def version(pkg):
    try: return md.version(pkg)
    except Exception: return "not_installed"

def spark_conf(key):
    try: return spark.conf.get(key)
    except Exception: return "unavailable"

payload = {
    "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    "python": platform.python_version(),
    "python_full": sys.version,
    "spark": getattr(spark, "version", "unknown"),
    "pyspark": version("pyspark"),
    "pandas": version("pandas"),
    "openpyxl": version("openpyxl"),
    "delta-spark": version("delta-spark"),
    "playwright": version("playwright"),
    "databricks_runtime_env": os.getenv("DATABRICKS_RUNTIME_VERSION", "unknown"),
    "databricks_spark_version": spark_conf("spark.databricks.clusterUsageTags.sparkVersion"),
    "spark_master": spark_conf("spark.master"),
    "spark_timezone": spark_conf("spark.sql.session.timeZone"),
}
print(json.dumps(payload, indent=2, ensure_ascii=False))

# Use a Unity Catalog volume path in the project workspace.
OUT = Path("/Volumes/camaronera_2026/oro/entregables/environment_lock.json")
OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"Environment lock: {OUT}")

# COMMAND ----------
