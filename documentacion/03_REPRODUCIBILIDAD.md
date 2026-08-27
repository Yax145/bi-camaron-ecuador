# 03. Reproducibilidad

La prueba computacional A/B final utiliza `reproducibility_test_v2_0`.

Alcance:

- CNA: lineage Bronze + Silver + Gold.
- BCE: Silver + Gold; adquisición previa externa.

Criterios de PASS:

1. `run_id` A y B diferentes para los cuatro pipelines.
2. mismo SHA-256 de snapshots de entrada;
3. misma configuración/contrato;
4. mismo entorno observado;
5. mismo conjunto de tablas;
6. mismas columnas;
7. mismos esquemas;
8. mismas cardinalidades;
9. mismos hashes canónicos del contenido.

Resultado auditado:

`REPRODUCIBILITY_PASS`

Tablas verificadas: 19.

Archivos:

- `baseline_A.json`;
- `repro_run_B.json`;
- `reproducibility_PASS.json`.
