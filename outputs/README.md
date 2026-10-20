# Outputs

Esta carpeta contiene salidas finales ligeras y útiles para revisar el proyecto sin volver a ejecutar los notebooks.

## `referencia_local/`

- tablas CSV del análisis de referencia;
- mapa Folium interactivo exportado a HTML.

## `depurado_local/`

- tablas CSV de calidad de datos, estabilidad y benchmark CSV frente a Parquet.

## `databricks/`

- tablas CSV de resultados, cobertura espacial, estrategia de ejecución, particiones, plan físico y portabilidad;
- mapa Folium interactivo exportado desde la implementación cloud.

## Parquet no versionado

Los notebooks también generan Parquet intermedios y analíticos. No se incluyen en Git por ser archivos regenerables y, en el caso del dataset depurado, por su tamaño.

Los directorios Parquet quedan cubiertos por `.gitignore`.

## Visualización de los mapas

Los HTML pueden abrirse directamente después de clonar el repositorio. Si se habilita GitHub Pages sobre `main`, también pueden servirse como contenido estático para una demo web interactiva.
