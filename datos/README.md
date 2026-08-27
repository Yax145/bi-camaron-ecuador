# Datos

Los datos raw institucionales no se incluyen en el repositorio.

Arquitectura de almacenamiento usada en Databricks:

```text
Catalog: camaronera_2026
Schema: bronce
Volumes:
- datoscna
- datosbce
- datoscfn
```

Las tablas refinadas se almacenan en:

- `camaronera_2026.plata`
- `camaronera_2026.oro`

Los snapshots auditados se identifican por SHA-256 en la evidencia de reproducibilidad.

BCE:
la adquisición previa se ejecuta fuera del alcance Databricks; el procesamiento reproducible auditado empieza en Silver.
