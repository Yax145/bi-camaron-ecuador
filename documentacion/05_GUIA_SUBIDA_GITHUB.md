# 05. Guía explícita para subir el proyecto a GitHub

Nombre recomendado del repositorio:

`bi-camaron-ecuador`

## PARTE A — Dejar Databricks ordenado

Antes de subir nada a GitHub, el Workspace debe quedar así:

```text
Workspace/
└── Camaronera/
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

En tus capturas actuales aparecen versiones antiguas `Gold_CNA_v2` y `Gold_BCE_v2`.
No forman parte del release final.

Opción recomendada:
1. crear dentro de `Camaronera` una carpeta llamada `Archivo`;
2. mover allí `Gold_CNA_v2`;
3. mover allí `Gold_BCE_v2`;
4. dejar en `Capa Gold` únicamente `Gold_CNA_v2_1` y `Gold_BCE_v2_1`.

No borrar una versión antigua si todavía deseas conservarla como histórico. Simplemente no subirla al release científico.

## PARTE B — Entender qué NO se exporta del Catalog

En Catalog Explorer existe:

```text
camaronera_2026
├── bronce
│   ├── datoscna
│   ├── datosbce
│   └── datoscfn
├── plata
└── oro
```

Eso NO se convierte en carpetas de GitHub.

- los Volumes contienen datos/archivos;
- `plata` contiene tablas Silver;
- `oro` contiene tablas Gold/entregables.

GitHub no almacena esas tablas Delta. Solo se documenta su existencia y se publican notebooks + evidencia.

## PARTE C — Exportar notebooks desde Databricks

Para cada notebook final:

1. abrir el notebook;
2. usar el menú del notebook (`File` o menú de opciones, según la interfaz);
3. seleccionar `Export`;
4. exportarlo como `Jupyter Notebook (.ipynb)` si la opción está disponible;
5. guardar el archivo con el nombre EXACTO indicado.

Exportar:

```text
Bronze_CNA.ipynb
Silver_CNA_v2_1_0.ipynb
Silver_BCE_v2_1_0.ipynb
Gold_CNA_v2_1.ipynb
Gold_BCE_v2_1.ipynb
capture_environment.ipynb
reproducibility_test_v2_0.ipynb
```

En este paquete esos archivos ya están colocados en sus carpetas correctas.

## PARTE D — Crear el repositorio en GitHub

1. entrar a GitHub;
2. pulsar `New repository`;
3. escribir:

Repository name:
`bi-camaron-ecuador`

Description:
`Sistema BI reproducible para el análisis de exportaciones del sector camaronero ecuatoriano mediante ELT y arquitectura Medallion.`

4. seleccionar `Public` si el artículo va a enlazar el código públicamente;
5. NO activar `Add a README file`;
6. NO crear `.gitignore` desde GitHub;
7. NO seleccionar licencia desde GitHub;
8. pulsar `Create repository`.

Este paquete ya contiene README, `.gitignore` y licencia.

## PARTE E — Subir las carpetas

Método recomendado: GitHub Desktop.

1. Descargar y descomprimir `bi-camaron-ecuador-github.zip`.
2. Abrir GitHub Desktop.
3. Iniciar sesión con la misma cuenta GitHub.
4. `File` → `Add local repository`.
5. Elegir la carpeta `bi-camaron-ecuador`.
6. Si indica que todavía no es repositorio Git:
   - elegir `create a repository here`;
   - nombre: `bi-camaron-ecuador`.
7. Revisar que aparezcan como cambios:
   - `notebooks/`
   - `scripts/`
   - `reproducibilidad/`
   - `evidencias/`
   - `documentacion/`
   - `datos/`
   - `README.md`
   - `requirements.txt`
   - `environment.yml`
   - `CITATION.cff`
   - `LICENSE`
   - `.gitignore`
8. Commit:
   `Publicación inicial del pipeline reproducible`
9. `Commit to main`.
10. `Publish repository`.
11. Confirmar que el repositorio publicado sea `bi-camaron-ecuador`.

## PARTE F — Verificación visual en GitHub

Al abrir GitHub, la raíz debe mostrar:

```text
notebooks
scripts
reproducibilidad
evidencias
documentacion
datos
README.md
requirements.txt
environment.yml
CITATION.cff
LICENSE
CHANGELOG.md
RELEASE_CHECKLIST.md
.gitignore
```

Dentro de `notebooks`:

```text
bronze/
silver/
gold/
```

No debe aparecer:

```text
Gold_CNA_v2
Gold_BCE_v2
Bronze_BCE
datos raw completos
tokens
contraseñas
.env
```

## PARTE G — Antes del release v1.0.0

1. editar `CITATION.cff`;
2. colocar autores finales;
3. colocar el owner/URL real de GitHub;
4. revisar `RELEASE_CHECKLIST.md`;
5. confirmar que `reproducibilidad/reproducibility_PASS.json` indique `REPRODUCIBILITY_PASS`;
6. crear el tag/release `v1.0.0` solo cuando Tableau y el artículo utilicen exactamente estos outputs finales.
