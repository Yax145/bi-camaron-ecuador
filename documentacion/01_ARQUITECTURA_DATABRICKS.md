# 01. Arquitectura de Databricks

## Workspace

El Workspace contiene notebooks y scripts. La estructura final debe ser:

```text
Workspace/Camaronera/
├── Capa Bronze/
│   └── Bronze_CNA
├── Capa Silver/
│   ├── Silver_CNA_v2_1_0
│   └── Silver_BCE_v2_1_0
├── Capa Gold/
│   ├── Gold_CNA_v2_1
│   └── Gold_BCE_v2_1
├── scripts/
│   ├── capture_environment
│   └── reproducibility_test_v2_0
└── Archivo/
    ├── Gold_CNA_v2
    └── Gold_BCE_v2
```

La carpeta `Archivo` es opcional. Su función es separar versiones anteriores del código final auditado.

## Unity Catalog

```text
camaronera_2026
├── bronce
│   ├── datoscna   [Volume]
│   ├── datosbce   [Volume]
│   └── datoscfn   [Volume]
├── plata
│   └── tablas Delta Silver
└── oro
    └── tablas Delta Gold + entregables
```

Workspace y Unity Catalog cumplen funciones diferentes:

- Workspace: código ejecutable.
- Unity Catalog: persistencia, metadatos, tablas y volúmenes.

No se replica el Unity Catalog como carpetas en GitHub.
