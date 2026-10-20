# Datos

Los datos originales **no se distribuyen en este repositorio**.

Para ejecutar los notebooks locales, coloca en esta carpeta copias obtenidas por una vía autorizada con estos nombres exactos:

```text
data/
├── yellow_tripdata_2015-01.csv
└── NYC_Community_Districts.geojson
```

## Viajes de taxi

Fuente oficial de referencia: NYC Taxi & Limousine Commission (TLC), Trip Record Data.

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

El proyecto fue desarrollado con una versión CSV de los viajes de taxi amarillo de enero de 2015. La publicación oficial actual puede ofrecer el histórico en Parquet; para una reproducción estricta debe utilizarse una copia compatible con el esquema esperado por los notebooks o adaptarse explícitamente la lectura.

## Distritos comunitarios

Fuente de referencia: NYC Open Data / Department of City Planning, Community Districts.

https://data.cityofnewyork.us/City-Government/Community-Districts/5crt-au7u

Los resultados geoespaciales del repositorio corresponden a la versión del GeoJSON utilizada en la ejecución final. Una versión más reciente de los límites podría producir pequeñas diferencias.

## Archivos generados

Los notebooks crean figuras en `images/` y resultados analíticos en `outputs/`. Los Parquet de gran tamaño están excluidos del control de versiones y se regeneran durante la ejecución.
