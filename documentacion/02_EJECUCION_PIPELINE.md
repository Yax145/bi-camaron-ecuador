# 02. Ejecución del pipeline

## CNA

1. Abrir `Workspace/Camaronera/Capa Bronze/Bronze_CNA`.
2. Ejecutar el notebook completo.
3. Verificar que genere/identifique el recurso CNA y su manifiesto.
4. Abrir `Workspace/Camaronera/Capa Silver/Silver_CNA_v2_1_0`.
5. Ejecutar completo.
6. Confirmar que los QA finalicen sin fallos críticos.
7. Abrir `Workspace/Camaronera/Capa Gold/Gold_CNA_v2_1`.
8. Ejecutar completo.
9. Confirmar que produzca los tres serving:
   - nacional;
   - destinos detallados;
   - regiones agregadas.
10. Descargar `Exportaciones_CNA_Gold_Tableau.xlsx`.

## BCE

1. La adquisición del XLSX BCE se realiza fuera de Databricks.
2. Copiar el snapshot final al volumen configurado bajo el esquema `bronce`.
3. Abrir `Workspace/Camaronera/Capa Silver/Silver_BCE_v2_1_0`.
4. Ejecutar completo.
5. Verificar cobertura enero 2021–junio 2026.
6. Abrir `Workspace/Camaronera/Capa Gold/Gold_BCE_v2_1`.
7. Ejecutar completo.
8. Descargar `Exportaciones_BCE_Gold_Tableau.xlsx`.

No existe un cutoff fijo de mayo en BCE.
