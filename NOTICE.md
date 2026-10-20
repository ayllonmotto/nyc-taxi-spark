# Avisos y materiales de terceros

## Autoría

El código, la documentación y la organización original de esta versión de portfolio son obra de **Carlos Ayllón Motto** y se distribuyen bajo la licencia indicada en `LICENSE`.

## Datos de viajes NYC Taxi

El análisis utiliza datos históricos de viajes de taxi amarillo de Nueva York correspondientes a enero de 2015.

Fuente oficial de referencia:

- NYC Taxi & Limousine Commission (TLC) · Trip Record Data  
  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

El notebook local utilizado en este proyecto espera una copia autorizada con el nombre:

```text
yellow_tripdata_2015-01.csv
```

**El dataset original no se incluye ni se redistribuye en este repositorio.** La publicación oficial actual puede ofrecer años históricos en formatos distintos del CSV utilizado durante el desarrollo.

## Cartografía de distritos comunitarios

El procesamiento geoespacial utiliza límites de distritos comunitarios de Nueva York.

Fuente de referencia:

- NYC Open Data / Department of City Planning · Community Districts  
  https://data.cityofnewyork.us/City-Government/Community-Districts/5crt-au7u

El proyecto espera localmente:

```text
NYC_Community_Districts.geojson
```

El fichero GeoJSON original no se redistribuye. Los mapas HTML incluidos son **salidas analíticas derivadas** y pueden contener geometrías transformadas procedentes de la fuente pública. La licencia MIT de este repositorio no debe interpretarse como una concesión de derechos sobre la cartografía original ni sobre otros datos de terceros.

## Salidas derivadas

Las imágenes y tablas incluidas en `images/` y `outputs/` han sido generadas por los notebooks finales y se publican para documentar los resultados del portfolio.

No se incluyen:

- el CSV original de viajes;
- el GeoJSON original de distritos comunitarios;
- el Parquet analítico depurado de millones de registros;
- copias de trabajo o materiales intermedios ajenos a la versión final del portfolio.

## Software de terceros

El proyecto utiliza software y librerías de terceros, entre ellos Apache Spark, Apache Sedona, Databricks, NumPy, pandas, PyArrow, GeoPandas, Shapely, Folium, Matplotlib y Seaborn. Cada proyecto conserva sus propias licencias, marcas y condiciones de uso.
