# Databricks notebook source

# Databricks notebook source
# Prueba de reproducibilidad computacional A/B — versión 2.0
# Proyecto: BI sector camaronero ecuatoriano
#
# Alcance de la prueba:
#   CNA: Silver (7 tablas) + Gold serving nacional/destinos/regiones
#   BCE: Silver (1 tabla) + Gold (8 tablas)
#
# Importante:
# - Para BCE, la adquisición previa se considera externa al alcance de esta prueba.
# - La propiedad técnica lineage.bronze_sha256 de BCE se interpreta aquí únicamente
#   como SHA-256 del snapshot XLSX de entrada consumido por Silver.
# - Un PASS exige NUEVOS run_id de Silver y Gold entre A y B.
# - No es posible obtener PASS ejecutando este notebook dos veces sobre las mismas
#   tablas sin volver a ejecutar los pipelines.

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import functions as F

# =============================================================================
# 1. CONFIGURACIÓN CONGELADA DEL ESTUDIO
# =============================================================================

TEST_SCHEMA_VERSION = "2.0"
CATALOG = "camaronera_2026"
SILVER_SCHEMA = "plata"
GOLD_SCHEMA = "oro"

# Snapshots auditados y congelados para esta prueba.
EXPECTED_CNA_INPUT_SHA256 = "1217b296e50e37f8aafd045079d79119a4123cf0805e0c01b1636f62c10613a1"
EXPECTED_BCE_INPUT_SHA256 = "2cb48f6bb83c56f6924d8f8233d03be2115b2e0d3000e3a359bccd25c8157672"

CNA_SILVER_TABLES = {
    "cna_silver_exportaciones_mensuales_dolares": f"{CATALOG}.{SILVER_SCHEMA}.exportaciones_mensuales_dolares_normalizada",
    "cna_silver_exportaciones_mensuales_libras": f"{CATALOG}.{SILVER_SCHEMA}.exportaciones_mensuales_libras_normalizada",
    "cna_silver_mercado_mayo": f"{CATALOG}.{SILVER_SCHEMA}.mercado_pais_mayo_normalizado",
    "cna_silver_mercado_acumulado": f"{CATALOG}.{SILVER_SCHEMA}.mercado_pais_acumulado_normalizado",
    "cna_silver_exportaciones_mensuales": f"{CATALOG}.{SILVER_SCHEMA}.exportaciones_mensuales",
    "cna_silver_mayo_historico": f"{CATALOG}.{SILVER_SCHEMA}.exportaciones_mayo_historico",
    "cna_silver_acumuladas_historico": f"{CATALOG}.{SILVER_SCHEMA}.exportaciones_acumuladas_historico",
}

CNA_GOLD_TABLES = {
    "cna_gold_nacional": f"{CATALOG}.{GOLD_SCHEMA}.cna_serving_nacional_mensual",
    "cna_gold_destinos": f"{CATALOG}.{GOLD_SCHEMA}.cna_serving_mercados",
    "cna_gold_regiones": f"{CATALOG}.{GOLD_SCHEMA}.cna_serving_regiones_agregadas",
}

BCE_SILVER_TABLES = {
    "bce_silver_exportaciones": f"{CATALOG}.{SILVER_SCHEMA}.bce_exportaciones_camaron_subpartida",
}

BCE_GOLD_TABLES = {
    "bce_gold_dim_fecha": f"{CATALOG}.{GOLD_SCHEMA}.bce_dim_fecha",
    "bce_gold_dim_producto": f"{CATALOG}.{GOLD_SCHEMA}.bce_dim_producto",
    "bce_gold_dim_subpartida": f"{CATALOG}.{GOLD_SCHEMA}.bce_dim_subpartida",
    "bce_gold_fact_exportaciones": f"{CATALOG}.{GOLD_SCHEMA}.bce_fact_exportaciones_subpartida",
    "bce_gold_resumen_mensual": f"{CATALOG}.{GOLD_SCHEMA}.bce_resumen_mensual",
    "bce_gold_resumen_anual": f"{CATALOG}.{GOLD_SCHEMA}.bce_resumen_anual",
    "bce_gold_indicadores_kpi": f"{CATALOG}.{GOLD_SCHEMA}.bce_indicadores_kpi",
    "bce_gold_serving_tableau": f"{CATALOG}.{GOLD_SCHEMA}.bce_serving_tableau",
}

ALL_TABLES = {
    **CNA_SILVER_TABLES,
    **CNA_GOLD_TABLES,
    **BCE_SILVER_TABLES,
    **BCE_GOLD_TABLES,
}

EXPECTED_ROWS = {
    "cna_silver_exportaciones_mensuales_dolares": 389,
    "cna_silver_exportaciones_mensuales_libras": 389,
    "cna_silver_mercado_mayo": 512,
    "cna_silver_mercado_acumulado": 589,
    "cna_silver_exportaciones_mensuales": 113,
    "cna_silver_mayo_historico": 7,
    "cna_silver_acumuladas_historico": 7,
    "cna_gold_nacional": 389,
    "cna_gold_destinos": 306,
    "cna_gold_regiones": 16,
    "bce_silver_exportaciones": 543,
    "bce_gold_dim_fecha": 66,
    "bce_gold_dim_producto": 1,
    "bce_gold_dim_subpartida": 14,
    "bce_gold_fact_exportaciones": 543,
    "bce_gold_resumen_mensual": 66,
    "bce_gold_resumen_anual": 6,
    "bce_gold_indicadores_kpi": 1,
    "bce_gold_serving_tableau": 543,
}

ROOT = Path(f"/Volumes/{CATALOG}/{GOLD_SCHEMA}/entregables/reproducibilidad_final_v2")
ROOT.mkdir(parents=True, exist_ok=True)
BASELINE_PATH = ROOT / "baseline_A.json"
PASS_PATH = ROOT / "reproducibility_PASS.json"

# Cambiar a True SOLO si se desea invalidar deliberadamente un baseline anterior.
RESET_BASELINE = False

# =============================================================================
# 2. UTILIDADES
# =============================================================================

def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()

def atomic_json_dump(payload: Dict[str, Any], path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    tmp.replace(path)

def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

def table_properties(table_ref: str) -> Dict[str, str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES {table_ref}").collect()
    return {str(r[0]): str(r[1]) for r in rows}

def canonical_table_fingerprint(table_ref: str) -> Dict[str, Any]:
    """
    Huella lógica independiente de la disposición física de archivos Delta.
    Ordena columnas y filas de forma determinista y serializa cada fila a JSON.
    """
    if not spark.catalog.tableExists(table_ref):
        raise RuntimeError(f"Tabla requerida no existe: {table_ref}")

    df = spark.table(table_ref)
    columns_original = list(df.columns)
    columns_canonical = sorted(columns_original)

    # Todas las tablas auditadas contienen tipos escalares ordenables.
    ordered = (
        df.select(*[F.col(c) for c in columns_canonical])
          .orderBy(*[F.col(c).asc_nulls_first() for c in columns_canonical])
    )

    json_rows = (
        ordered
        .select(
            F.to_json(
                F.struct(*[F.col(c) for c in columns_canonical]),
                {"ignoreNullFields": "false"},
            ).alias("canonical_json")
        )
        .toLocalIterator()
    )

    digest = hashlib.sha256()
    row_count = 0
    for row in json_rows:
        payload = (row["canonical_json"] or "null") + "\n"
        digest.update(payload.encode("utf-8"))
        row_count += 1

    schema_json = df.schema.json()

    return {
        "table_ref": table_ref,
        "rows": row_count,
        "columns": columns_original,
        "canonical_columns": columns_canonical,
        "schema_json": schema_json,
        "schema_sha256": sha256_text(schema_json),
        "sha256_canonical": digest.hexdigest(),
    }

def recursive_status_counts(obj: Any) -> Dict[str, int]:
    counts = {"FAIL": 0, "WARN": 0}
    def walk(x: Any) -> None:
        if isinstance(x, dict):
            status = str(x.get("status", "")).strip().upper()
            if status == "FAIL":
                counts["FAIL"] += 1
            elif status in {"WARN", "WARNING"}:
                counts["WARN"] += 1
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk(obj)
    return counts

def manifest_evidence(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"Manifiesto requerido no existe: {path}")
    payload = load_json(path)
    raw = path.read_bytes()
    statuses = recursive_status_counts(payload)
    software = payload.get("software_versions") or {}
    return {
        "path": str(path),
        "sha256_file": sha256_bytes(raw),
        "run_id": str(payload.get("run_id") or ""),
        "pipeline_version": str(payload.get("pipeline_version") or ""),
        "config_hash": payload.get("config_hash"),
        "software_versions": software,
        "software_versions_sha256": sha256_text(
            json.dumps(software, sort_keys=True, ensure_ascii=False)
        ),
        "fail_status_count": statuses["FAIL"],
        "warn_status_count": statuses["WARN"],
    }

def require_equal(label: str, actual: Any, expected: Any) -> None:
    if actual != expected:
        raise RuntimeError(f"{label}: esperado={expected!r}, obtenido={actual!r}")

def require_nonempty(label: str, value: str) -> None:
    if not str(value or "").strip():
        raise RuntimeError(f"{label}: valor vacío o no disponible.")

def layer_properties(table_refs: Dict[str, str], run_property: str) -> Dict[str, Any]:
    props_by_name = {name: table_properties(ref) for name, ref in table_refs.items()}

    run_ids = {p.get(run_property, "").strip() for p in props_by_name.values()}
    versions = {p.get("pipeline.version", "").strip() for p in props_by_name.values()}
    input_shas = {p.get("lineage.bronze_sha256", "").strip() for p in props_by_name.values()}

    if len(run_ids) != 1:
        raise RuntimeError(f"Run IDs inconsistentes en capa: {run_ids}")
    if len(versions) != 1:
        raise RuntimeError(f"Versiones inconsistentes en capa: {versions}")
    if len(input_shas) != 1:
        raise RuntimeError(f"SHA de entrada inconsistentes en capa: {input_shas}")

    run_id = next(iter(run_ids))
    version = next(iter(versions))
    input_sha = next(iter(input_shas))

    require_nonempty(run_property, run_id)
    require_nonempty("pipeline.version", version)
    require_nonempty("input_snapshot_sha256", input_sha)

    return {
        "run_id": run_id,
        "pipeline_version": version,
        "input_snapshot_sha256": input_sha,
        "properties_by_table": props_by_name,
    }

# =============================================================================
# 3. CAPTURA DE EVIDENCIA DE LA EJECUCIÓN ACTUAL
# =============================================================================

cna_silver = layer_properties(CNA_SILVER_TABLES, "pipeline.silver_run_id")
cna_gold = layer_properties(CNA_GOLD_TABLES, "pipeline.gold_run_id")
bce_silver = layer_properties(BCE_SILVER_TABLES, "pipeline.silver_run_id")
bce_gold = layer_properties(BCE_GOLD_TABLES, "pipeline.gold_run_id")

# Congelación de snapshots de entrada del estudio.
require_equal("CNA input SHA-256", cna_silver["input_snapshot_sha256"], EXPECTED_CNA_INPUT_SHA256)
require_equal("CNA Gold input SHA-256", cna_gold["input_snapshot_sha256"], EXPECTED_CNA_INPUT_SHA256)
require_equal("BCE input SHA-256", bce_silver["input_snapshot_sha256"], EXPECTED_BCE_INPUT_SHA256)
require_equal("BCE Gold input SHA-256", bce_gold["input_snapshot_sha256"], EXPECTED_BCE_INPUT_SHA256)

# Lineage interno: Gold debe proceder exactamente del Silver de ESTA ejecución.
cna_gold_props = next(iter(cna_gold["properties_by_table"].values()))
bce_gold_props = next(iter(bce_gold["properties_by_table"].values()))

require_equal(
    "CNA Gold → Silver run",
    cna_gold_props.get("lineage.silver_run_id", "").strip(),
    cna_silver["run_id"],
)
require_equal(
    "BCE Gold → Silver run",
    bce_gold_props.get("lineage.silver_run_id", "").strip(),
    bce_silver["run_id"],
)

# Configuración:
# CNA Silver: hash nativo almacenado en su manifiesto.
# CNA/BCE Gold: hash nativo gold.config_sha256.
# BCE Silver: el notebook actual no persiste un config_hash propio; se construye
# un hash de contrato reproducible a partir de versión, schema, llave de negocio,
# fingerprint estructural y unidades declaradas. Se reporta explícitamente como
# config_contract_sha256, no como hash del código fuente.
cna_silver_manifest_path = Path(
    f"/Volumes/{CATALOG}/{SILVER_SCHEMA}/metadatos_cna/"
    f"silver_manifest_{cna_silver['run_id']}_CNA.json"
)
cna_gold_manifest_path = Path(
    f"/Volumes/{CATALOG}/{GOLD_SCHEMA}/metadatos_cna_gold/"
    f"gold_manifest_{cna_gold['run_id']}_CNA.json"
)
bce_silver_manifest_path = Path(
    f"/Volumes/{CATALOG}/{SILVER_SCHEMA}/metadatos_bce/"
    f"silver_manifest_{bce_silver['run_id']}_BCE.json"
)
bce_gold_manifest_path = Path(
    f"/Volumes/{CATALOG}/{GOLD_SCHEMA}/metadatos_bce_gold/"
    f"gold_manifest_{bce_gold['run_id']}_BCE.json"
)

manifests = {
    "cna_silver": manifest_evidence(cna_silver_manifest_path),
    "cna_gold": manifest_evidence(cna_gold_manifest_path),
    "bce_silver": manifest_evidence(bce_silver_manifest_path),
    "bce_gold": manifest_evidence(bce_gold_manifest_path),
}

if manifests["cna_silver"]["fail_status_count"] != 0:
    raise RuntimeError("El manifiesto Silver CNA contiene estados FAIL.")
if manifests["cna_gold"]["fail_status_count"] != 0:
    raise RuntimeError("El manifiesto Gold CNA contiene estados FAIL.")
if manifests["bce_silver"]["fail_status_count"] != 0:
    raise RuntimeError("El manifiesto Silver BCE contiene estados FAIL.")
if manifests["bce_gold"]["fail_status_count"] != 0:
    raise RuntimeError("El manifiesto Gold BCE contiene estados FAIL.")

cna_silver_config_hash = manifests["cna_silver"].get("config_hash")
require_nonempty("CNA Silver config_hash", cna_silver_config_hash)

cna_gold_config_hashes = {
    p.get("gold.config_sha256", "").strip()
    for p in cna_gold["properties_by_table"].values()
}
if len(cna_gold_config_hashes) != 1:
    raise RuntimeError(f"CNA Gold config hashes inconsistentes: {cna_gold_config_hashes}")
cna_gold_config_hash = next(iter(cna_gold_config_hashes))
require_nonempty("CNA Gold config SHA-256", cna_gold_config_hash)

bce_gold_config_hashes = {
    p.get("gold.config_sha256", "").strip()
    for p in bce_gold["properties_by_table"].values()
}
if len(bce_gold_config_hashes) != 1:
    raise RuntimeError(f"BCE Gold config hashes inconsistentes: {bce_gold_config_hashes}")
bce_gold_config_hash = next(iter(bce_gold_config_hashes))
require_nonempty("BCE Gold config SHA-256", bce_gold_config_hash)

bce_silver_ref = next(iter(BCE_SILVER_TABLES.values()))
bce_silver_df = spark.table(bce_silver_ref)
bce_silver_props = next(iter(bce_silver["properties_by_table"].values()))

bce_silver_contract_payload = {
    "pipeline_version": bce_silver["pipeline_version"],
    "table_ref": bce_silver_ref,
    "schema_json": bce_silver_df.schema.json(),
    "business_key": [
        "fecha_periodo",
        "codigo_producto_principal",
        "codigo_subpartida",
    ],
    "source_report": bce_silver_props.get("source.report", ""),
    "source_fob_unit": bce_silver_props.get("source.fob_unit", ""),
    "canonical_fob_unit": bce_silver_props.get("canonical.fob_unit", ""),
    "workbook_structure_fingerprint": bce_silver_props.get(
        "lineage.workbook_structure_fingerprint", ""
    ),
}
bce_silver_config_contract_hash = sha256_text(
    json.dumps(bce_silver_contract_payload, sort_keys=True, ensure_ascii=False)
)

# Hash lógico de TODAS las tablas incluidas.
table_evidence: Dict[str, Any] = {}
for logical_name, table_ref in ALL_TABLES.items():
    evidence = canonical_table_fingerprint(table_ref)
    expected_rows = EXPECTED_ROWS[logical_name]
    require_equal(f"{logical_name}.rows", evidence["rows"], expected_rows)
    table_evidence[logical_name] = evidence
    print(
        f"PASS | {logical_name:42s} | "
        f"{evidence['rows']:4d} filas | "
        f"{evidence['sha256_canonical'][:16]}..."
    )

# Contratos semánticos críticos CNA.
cna_destinos = spark.table(CNA_GOLD_TABLES["cna_gold_destinos"])
cna_regiones = spark.table(CNA_GOLD_TABLES["cna_gold_regiones"])

for required_col in ["destino", "destino_fuente", "nivel_destino"]:
    if required_col not in cna_destinos.columns:
        raise RuntimeError(f"CNA destinos no contiene columna requerida: {required_col}")

if "pais" in cna_destinos.columns:
    raise RuntimeError("CNA Gold serving de destinos volvió a exponer la columna 'pais'.")

if cna_destinos.filter(F.col("nivel_destino") != F.lit("DETALLADO")).count() != 0:
    raise RuntimeError("CNA destinos contiene filas que no son DETALLADO.")

if cna_regiones.filter(F.col("nivel_destino") != F.lit("AGREGADO_REGIONAL")).count() != 0:
    raise RuntimeError("CNA regiones contiene filas que no son AGREGADO_REGIONAL.")

snapshot = {
    "test_schema_version": TEST_SCHEMA_VERSION,
    "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    "study_scope": {
        "cna": "Bronze lineage + Silver + Gold",
        "bce": "Silver + Gold; adquisición previa externa a la prueba Databricks",
    },
    "expected_input_sha256": {
        "CNA": EXPECTED_CNA_INPUT_SHA256,
        "BCE": EXPECTED_BCE_INPUT_SHA256,
    },
    "pipelines": {
        "cna_silver": {
            "run_id": cna_silver["run_id"],
            "pipeline_version": cna_silver["pipeline_version"],
            "input_snapshot_sha256": cna_silver["input_snapshot_sha256"],
            "config_sha256": cna_silver_config_hash,
            "environment_sha256": manifests["cna_silver"]["software_versions_sha256"],
        },
        "cna_gold": {
            "run_id": cna_gold["run_id"],
            "pipeline_version": cna_gold["pipeline_version"],
            "silver_run_id": cna_gold_props.get("lineage.silver_run_id", "").strip(),
            "input_snapshot_sha256": cna_gold["input_snapshot_sha256"],
            "config_sha256": cna_gold_config_hash,
            "environment_sha256": manifests["cna_gold"]["software_versions_sha256"],
        },
        "bce_silver": {
            "run_id": bce_silver["run_id"],
            "pipeline_version": bce_silver["pipeline_version"],
            "input_snapshot_sha256": bce_silver["input_snapshot_sha256"],
            "config_contract_sha256": bce_silver_config_contract_hash,
            "environment_sha256": manifests["bce_silver"]["software_versions_sha256"],
            "note": (
                "config_contract_sha256 es una huella del contrato observado. "
                "El Silver BCE actual no persiste un config_hash nativo del notebook."
            ),
        },
        "bce_gold": {
            "run_id": bce_gold["run_id"],
            "pipeline_version": bce_gold["pipeline_version"],
            "silver_run_id": bce_gold_props.get("lineage.silver_run_id", "").strip(),
            "input_snapshot_sha256": bce_gold["input_snapshot_sha256"],
            "config_sha256": bce_gold_config_hash,
            "environment_sha256": manifests["bce_gold"]["software_versions_sha256"],
        },
    },
    "manifests": manifests,
    "tables": table_evidence,
}

# =============================================================================
# 4. BASELINE A O COMPARACIÓN B
# =============================================================================

if RESET_BASELINE and BASELINE_PATH.exists():
    BASELINE_PATH.unlink()
    if PASS_PATH.exists():
        PASS_PATH.unlink()
    print("RESET_BASELINE=True → baseline anterior eliminado.")

run_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
current_path = ROOT / f"repro_run_{run_tag}.json"
atomic_json_dump(snapshot, current_path)

if not BASELINE_PATH.exists():
    atomic_json_dump(snapshot, BASELINE_PATH)
    print("\n" + "=" * 100)
    print("BASELINE_A_CREATED")
    print("=" * 100)
    print(f"Baseline A : {BASELINE_PATH}")
    print(f"Run actual : {current_path}")
    print("\nAHORA DEBES VOLVER A EJECUTAR, EN ESTE ORDEN:")
    print("1. Silver_CNA_v2_1_0")
    print("2. Gold_CNA_v2_1")
    print("3. Silver_BCE_v2_1_0")
    print("4. Gold_BCE_v2_1")
    print("5. Este notebook reproducibility_test_v2_0")
    print("\nNo cambies los snapshots de entrada ni la configuración entre A y B.")
else:
    baseline = load_json(BASELINE_PATH)
    require_equal(
        "test_schema_version",
        snapshot["test_schema_version"],
        baseline["test_schema_version"],
    )

    comparison: List[Dict[str, Any]] = []

    def check(label: str, a: Any, b: Any) -> None:
        passed = (a == b)
        comparison.append({
            "check": label,
            "status": "PASS" if passed else "FAIL",
            "baseline_A": a,
            "run_B": b,
        })
        if not passed:
            raise RuntimeError(f"REPRODUCIBILITY_FAIL | {label}: A={a!r}, B={b!r}")

    # A y B DEBEN provenir de ejecuciones diferentes.
    for pipeline_name in ["cna_silver", "cna_gold", "bce_silver", "bce_gold"]:
        run_a = baseline["pipelines"][pipeline_name]["run_id"]
        run_b = snapshot["pipelines"][pipeline_name]["run_id"]
        if run_a == run_b:
            raise RuntimeError(
                "PIPELINE_NOT_REEXECUTED | "
                f"{pipeline_name}: run_id A y B son idénticos ({run_a}). "
                "Debes volver a ejecutar Silver/Gold antes de ejecutar la prueba B."
            )
        comparison.append({
            "check": f"{pipeline_name}.run_id_changed",
            "status": "PASS",
            "baseline_A": run_a,
            "run_B": run_b,
        })

    # Mismos snapshots de entrada.
    for source in ["CNA", "BCE"]:
        check(
            f"{source}.input_snapshot_sha256",
            baseline["expected_input_sha256"][source],
            snapshot["expected_input_sha256"][source],
        )

    # Misma configuración/contrato y mismo entorno observado por pipeline.
    for pipeline_name in ["cna_silver", "cna_gold", "bce_gold"]:
        check(
            f"{pipeline_name}.config_sha256",
            baseline["pipelines"][pipeline_name]["config_sha256"],
            snapshot["pipelines"][pipeline_name]["config_sha256"],
        )

    check(
        "bce_silver.config_contract_sha256",
        baseline["pipelines"]["bce_silver"]["config_contract_sha256"],
        snapshot["pipelines"]["bce_silver"]["config_contract_sha256"],
    )

    for pipeline_name in ["cna_silver", "cna_gold", "bce_silver", "bce_gold"]:
        check(
            f"{pipeline_name}.pipeline_version",
            baseline["pipelines"][pipeline_name]["pipeline_version"],
            snapshot["pipelines"][pipeline_name]["pipeline_version"],
        )
        check(
            f"{pipeline_name}.environment_sha256",
            baseline["pipelines"][pipeline_name]["environment_sha256"],
            snapshot["pipelines"][pipeline_name]["environment_sha256"],
        )

    # Misma estructura y mismo contenido lógico.
    check("table_set", sorted(baseline["tables"]), sorted(snapshot["tables"]))

    for logical_name in sorted(snapshot["tables"]):
        a = baseline["tables"][logical_name]
        b = snapshot["tables"][logical_name]
        check(f"{logical_name}.rows", a["rows"], b["rows"])
        check(f"{logical_name}.columns", a["columns"], b["columns"])
        check(f"{logical_name}.schema_sha256", a["schema_sha256"], b["schema_sha256"])
        check(
            f"{logical_name}.sha256_canonical",
            a["sha256_canonical"],
            b["sha256_canonical"],
        )

    report = {
        "status": "REPRODUCIBILITY_PASS",
        "test_schema_version": TEST_SCHEMA_VERSION,
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_A_path": str(BASELINE_PATH),
        "run_B_path": str(current_path),
        "baseline_A_run_ids": {
            k: v["run_id"] for k, v in baseline["pipelines"].items()
        },
        "run_B_run_ids": {
            k: v["run_id"] for k, v in snapshot["pipelines"].items()
        },
        "checks": comparison,
        "tables_verified": len(snapshot["tables"]),
        "input_snapshots": snapshot["expected_input_sha256"],
    }
    atomic_json_dump(report, PASS_PATH)

    print("\n" + "=" * 100)
    print("REPRODUCIBILITY_PASS")
    print("=" * 100)
    print(f"Tablas verificadas : {len(snapshot['tables'])}")
    print(f"Checks ejecutados  : {len(comparison)}")
    print(f"Baseline A         : {BASELINE_PATH}")
    print(f"Ejecución B        : {current_path}")
    print(f"Evidencia PASS     : {PASS_PATH}")
    print("\nLa ejecución B tiene run_id distintos y reproduce:")
    print("- los mismos snapshots de entrada;")
    print("- la misma configuración/contrato;")
    print("- el mismo entorno de software observado;")
    print("- los mismos esquemas y cardinalidades;")
    print("- los mismos hashes canónicos de contenido.")


# COMMAND ----------
