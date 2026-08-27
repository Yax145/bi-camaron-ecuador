# BI Camarón Ecuador

Sistema de inteligencia de negocios para la adquisición, procesamiento y análisis coordinado de datos de exportación del sector camaronero ecuatoriano mediante ELT y arquitectura Medallion.

**Tecnologías:** Databricks, PySpark, Delta Lake, Unity Catalog, Python, OpenPyXL y Tableau.

## 1. Qué contiene este repositorio

Este repositorio publica únicamente:

- código fuente de los notebooks finales;
- scripts auxiliares;
- evidencia de las ejecuciones finales;
- prueba controlada de reproducibilidad A/B;
- archivos serving finales para Tableau;
- documentación de la arquitectura y del orden de ejecución.

Los archivos raw institucionales no se versionan en GitHub. Se documentan mediante su ubicación lógica y huellas SHA-256.

## 2. Estructura REAL en Databricks

En Databricks hay dos estructuras distintas que no deben confundirse:

### A. Workspace = código y notebooks

La estructura final recomendada del Workspace es:

```text
Workspace/
└── Camaronera/
    ├── Capa Bronze/
    │   └── Bronze_CNA
    │
    ├── Capa Silver/
    │   ├── Silver_CNA_v2_1_0
    │   └── Silver_BCE_v2_1_0
    │
    ├── Capa Gold/
    │   ├── Gold_CNA_v2_1
    │   └── Gold_BCE_v2_1
    │
    ├── scripts/
    │   ├── capture_environment
    │   └── reproducibility_test_v2_0
    │
    └── Archivo/
        ├── Gold_CNA_v2
        └── Gold_BCE_v2
```

`Archivo/` es opcional, pero se recomienda para mover allí notebooks antiguos. Los notebooks antiguos NO se incluyen en el release de GitHub.

No existe un notebook Bronze BCE en este alcance. La adquisición BCE se realiza externamente y Databricks comienza su procesamiento reproducible en Silver.

### B. Unity Catalog = datos, tablas y volúmenes

La persistencia de datos se encuentra en:

```text
Catalog: camaronera_2026
├── bronce
│   ├── Volume: datoscna
│   ├── Volume: datosbce
│   └── Volume: datoscfn
│
├── plata
│   └── tablas Delta Silver
│
└── oro
    └── tablas Delta Gold y entregables
```

IMPORTANTE: `bronce`, `plata` y `oro` del Unity Catalog NO son carpetas que deban copiarse a GitHub. GitHub guarda el código y la documentación, no las tablas Delta ni los volúmenes del catálogo.

## 3. Correspondencia Databricks → GitHub

| Databricks Workspace | GitHub |
|---|---|
| `Camaronera/Capa Bronze/Bronze_CNA` | `notebooks/bronze/Bronze_CNA.ipynb` |
| `Camaronera/Capa Silver/Silver_CNA_v2_1_0` | `notebooks/silver/Silver_CNA_v2_1_0.ipynb` |
| `Camaronera/Capa Silver/Silver_BCE_v2_1_0` | `notebooks/silver/Silver_BCE_v2_1_0.ipynb` |
| `Camaronera/Capa Gold/Gold_CNA_v2_1` | `notebooks/gold/Gold_CNA_v2_1.ipynb` |
| `Camaronera/Capa Gold/Gold_BCE_v2_1` | `notebooks/gold/Gold_BCE_v2_1.ipynb` |
| `Camaronera/scripts/capture_environment` | `scripts/capture_environment.ipynb` |
| `Camaronera/scripts/reproducibility_test_v2_0` | `reproducibilidad/reproducibility_test_v2_0.ipynb` |

## 4. Estructura del repositorio GitHub

```text
bi-camaron-ecuador/
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── scripts/
├── reproducibilidad/
├── evidencias/
│   ├── ejecuciones/
│   ├── serving/
│   ├── entorno/
│   └── tableau/
├── documentacion/
├── datos/
├── README.md
├── requirements.txt
├── environment.yml
├── CITATION.cff
├── LICENSE
├── CHANGELOG.md
├── RELEASE_CHECKLIST.md
└── .gitignore
```

## 5. Orden exacto de ejecución

### CNA

1. `Bronze_CNA`
2. `Silver_CNA_v2_1_0`
3. `Gold_CNA_v2_1`

### BCE

1. colocar el snapshot XLSX de entrada en el volumen esperado;
2. `Silver_BCE_v2_1_0`;
3. `Gold_BCE_v2_1`.

### Reproducibilidad

La prueba A/B final ya fue ejecutada. El resultado formal fue:

- `REPRODUCIBILITY_PASS`;
- 19 tablas verificadas;
- ejecuciones A y B con `run_id` diferentes;
- mismos snapshots de entrada;
- misma configuración/contrato;
- mismo entorno observado;
- mismos esquemas, cardinalidades y hashes canónicos.

Consulte `documentacion/03_REPRODUCIBILIDAD.md`.

## 6. Qué NO debe subirse a GitHub

No subir:

- credenciales;
- tokens;
- contraseñas;
- `.env`;
- archivos raw institucionales sin autorización;
- tablas Delta;
- carpetas físicas de Unity Catalog;
- notebooks antiguos;
- ZIPs de pruebas previas;
- versiones con cutoff BCE a mayo de 2026.

## 7. Cómo subirlo

La guía detallada y paso a paso está en:

`documentacion/05_GUIA_SUBIDA_GITHUB.md`

Nombre recomendado del repositorio:

`bi-camaron-ecuador`

Descripción recomendada:

`Sistema BI reproducible para el análisis de exportaciones del sector camaronero ecuatoriano mediante ELT y arquitectura Medallion.`

## 8. Evidencia de reproducibilidad

Las evidencias se encuentran en `reproducibilidad/` y `evidencias/ejecuciones/`.

El release científico debe congelarse únicamente con estos notebooks finales.
