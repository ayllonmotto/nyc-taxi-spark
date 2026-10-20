import json

import folium
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from pathlib import Path

from pyspark.sql import DataFrame as DF
from pyspark.sql import Window
from pyspark.sql import functions as F


# ============================================================
# CONSTANTES
# ============================================================
DAYS_ORDER = [
    'lunes',
    'martes',
    'miercoles',
    'jueves',
    'viernes',
    'sabado',
    'domingo'
]

DAY_LABELS = {
    'lunes': 'Lunes',
    'martes': 'Martes',
    'miercoles': 'Miércoles',
    'jueves': 'Jueves',
    'viernes': 'Viernes',
    'sabado': 'Sábado',
    'domingo': 'Domingo',
}


DISTRICTS_REFERENCE = """101,Financial District (CD1)
102,Greenwich Village and Soho (CD2)
103,Lower East Side and Chinatown (CD3)
104,Clinton and Chelsea (CD4)
105,Midtown (CD5)
106,Stuyvesant Town and Turtle Bay (CD6)
107,Upper West Side (CD7)
108,Upper East Side (CD8)
109,Morningside Heights and Hamilton Heights (CD9)
110,Central Harlem (CD10)
111,East Harlem (CD11)
112,Washington Heights and Inwood (CD12)
201,Mott Haven and Melrose (CD1)
202,Hunts Point and Longwood (CD2)
203,Morrisania and Crotona (CD3)
204,Highbridge and Concourse (CD4)
205,Fordham and University Heights (CD5)
206,Belmont and East Tremont (CD6)
207,Kingsbridge Heights and Bedford (CD7)
208,Riverdale and Fieldston (CD8)
209,Parkchester and Soundview (CD9)
210,Throgs Neck and Co-op City (CD10)
211,Morris Park and Bronxdale (CD11)
212,Williamsbridge and Baychester (CD12)
301,Greenpoint and Williamsburg (CD1)
302,Fort Greene and Brooklyn Heights (CD2)
303,Bedford Stuyvesant (CD3)
304,Bushwick (CD4)
305,East New York and Starrett City (CD5)
306,Park Slope and Carroll Gardens (CD6)
307,Sunset Park (CD7)
308,Crown Heights and Prospect Heights (CD8)
309,South Crown Heights and Lefferts Gardens (CD9)
310,Bay Ridge and Dyker Heights (CD10)
311,Bensonhurst (CD11)
312,Borough Park (CD12)
313,Coney Island (CD13)
314,Flatbush and Midwood (CD14)
315,Sheepshead Bay (CD15)
316,Brownsville (CD16)
317,East Flatbush (CD17)
318,Flatlands and Canarsie (CD18)
401,Long Island City and Astoria (CD1)
402,Woodside and Sunnyside (CD2)
403,Jackson Heights (CD3)
404,Elmhurst and Corona (CD4)
405,Ridgewood and Maspeth (CD5)
406,Rego Park and Forest Hills (CD6)
407,Flushing and Whitestone (CD7)
408,Hillcrest and Fresh Meadows (CD8)
409,Kew Gardens and Woodhaven (CD9)
410,South Ozone Park and Howard Beach (CD10)
411,Bayside and Little Neck (CD11)
412,Jamaica and Hollis (CD12)
413,Queens Village (CD13)
414,Rockaway and Broad Channel (CD14)
501,St. George and Stapleton (CD1)
502,South Beach and Willowbrook (CD2)
503,Tottenville and Great Kills (CD3)"""


DISTRICT_LABELS = {
    # Manhattan
    'Financial District (CD1)': 'Financial District',
    'Greenwich Village and Soho (CD2)': 'Greenwich / SoHo',
    'Lower East Side and Chinatown (CD3)': 'LES / Chinatown',
    'Clinton and Chelsea (CD4)': 'Chelsea / Clinton',
    'Midtown (CD5)': 'Midtown',
    'Stuyvesant Town and Turtle Bay (CD6)': 'Stuyvesant / Turtle Bay',
    'Upper West Side (CD7)': 'Upper West Side',
    'Upper East Side (CD8)': 'Upper East Side',
    'Morningside Heights and Hamilton Heights (CD9)': 'Morningside / Hamilton Hts',
    'Central Harlem (CD10)': 'Central Harlem',
    'East Harlem (CD11)': 'East Harlem',
    'Washington Heights and Inwood (CD12)': 'Washington Hts / Inwood',

    # Bronx
    'Mott Haven and Melrose (CD1)': 'Mott Haven / Melrose',
    'Hunts Point and Longwood (CD2)': 'Hunts Point / Longwood',
    'Morrisania and Crotona (CD3)': 'Morrisania / Crotona',
    'Highbridge and Concourse (CD4)': 'Highbridge / Concourse',
    'Fordham and University Heights (CD5)': 'Fordham / University Hts',
    'Belmont and East Tremont (CD6)': 'Belmont / East Tremont',
    'Kingsbridge Heights and Bedford (CD7)': 'Kingsbridge Hts / Bedford',
    'Riverdale and Fieldston (CD8)': 'Riverdale / Fieldston',
    'Parkchester and Soundview (CD9)': 'Parkchester / Soundview',
    'Throgs Neck and Co-op City (CD10)': 'Throgs Neck / Co-op City',
    'Morris Park and Bronxdale (CD11)': 'Morris Park / Bronxdale',
    'Williamsbridge and Baychester (CD12)': 'Williamsbridge / Baychester',

    # Brooklyn
    'Greenpoint and Williamsburg (CD1)': 'Greenpoint / Williamsburg',
    'Fort Greene and Brooklyn Heights (CD2)': 'Fort Greene / Brooklyn Hts',
    'Bedford Stuyvesant (CD3)': 'Bedford-Stuyvesant',
    'Bushwick (CD4)': 'Bushwick',
    'East New York and Starrett City (CD5)': 'East NY / Starrett City',
    'Park Slope and Carroll Gardens (CD6)': 'Park Slope / Carroll Gdns',
    'Sunset Park (CD7)': 'Sunset Park',
    'Crown Heights and Prospect Heights (CD8)': 'Crown / Prospect Hts',
    'South Crown Heights and Lefferts Gardens (CD9)': 'S. Crown Hts / Lefferts',
    'Bay Ridge and Dyker Heights (CD10)': 'Bay Ridge / Dyker Hts',
    'Bensonhurst (CD11)': 'Bensonhurst',
    'Borough Park (CD12)': 'Borough Park',
    'Coney Island (CD13)': 'Coney Island',
    'Flatbush and Midwood (CD14)': 'Flatbush / Midwood',
    'Sheepshead Bay (CD15)': 'Sheepshead Bay',
    'Brownsville (CD16)': 'Brownsville',
    'East Flatbush (CD17)': 'East Flatbush',
    'Flatlands and Canarsie (CD18)': 'Flatlands / Canarsie',

    # Queens
    'Long Island City and Astoria (CD1)': 'LIC / Astoria',
    'Woodside and Sunnyside (CD2)': 'Woodside / Sunnyside',
    'Jackson Heights (CD3)': 'Jackson Heights',
    'Elmhurst and Corona (CD4)': 'Elmhurst / Corona',
    'Ridgewood and Maspeth (CD5)': 'Ridgewood / Maspeth',
    'Rego Park and Forest Hills (CD6)': 'Rego Park / Forest Hills',
    'Flushing and Whitestone (CD7)': 'Flushing / Whitestone',
    'Hillcrest and Fresh Meadows (CD8)': 'Hillcrest / Fresh Meadows',
    'Kew Gardens and Woodhaven (CD9)': 'Kew Gardens / Woodhaven',
    'South Ozone Park and Howard Beach (CD10)': 'S. Ozone Park / Howard Bch',
    'Bayside and Little Neck (CD11)': 'Bayside / Little Neck',
    'Jamaica and Hollis (CD12)': 'Jamaica / Hollis',
    'Queens Village (CD13)': 'Queens Village',
    'Rockaway and Broad Channel (CD14)': 'Rockaway / Broad Channel',

    # Staten Island
    'St. George and Stapleton (CD1)': 'St. George / Stapleton',
    'South Beach and Willowbrook (CD2)': 'South Beach / Willowbrook',
    'Tottenville and Great Kills (CD3)': 'Tottenville / Great Kills',
}


# ============================================================
# FUNCIONES ESTADÍSTICAS
# ============================================================
def compute_boxplot_stats(df: DF, col_name: str) -> DF:
    """
    Calcula los estadísticos necesarios para construir boxplots
    por hora de recogida y día de la semana.

    Para cada combinación de pickup_hour y day_of_week obtiene:

    - primer cuartil;
    - mediana;
    - tercer cuartil;
    - bigote inferior;
    - bigote superior.
    """
    required_columns = {
        'pickup_hour',
        'day_of_week',
        col_name
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f'Faltan columnas necesarias para calcular los boxplots: {sorted(missing_columns)}'
        )

    # La ventana conserva las observaciones originales dentro de cada combinación de hora y día
    window = Window.partitionBy('pickup_hour', 'day_of_week')

    # El IQR define los límites de Tukey utilizados para los bigotes
    iqr = F.col('q3') - F.col('q1')

    stat_df = (
        df
        .filter(F.col(col_name).isNotNull())
        # Cuartiles y mediana dentro de cada ventana temporal
        .withColumn('q1', F.percentile_approx(col_name, 0.25).over(window))
        .withColumn('median', F.median(col_name).over(window))
        .withColumn('q3', F.percentile_approx(col_name, 0.75).over(window))

        # Identificación de observaciones válidas para los bigotes
        .withColumn('valid_lower_whisker', F.col(col_name) >= F.col('q1') - 1.5 * iqr)
        .withColumn('valid_upper_whisker', F.col(col_name) <= F.col('q3') + 1.5 * iqr)
        # Una fila por hora y cinco estadísticos por cada día
        .groupBy('pickup_hour')
        .pivot('day_of_week', DAYS_ORDER)
        .agg(
            F.first('q1').alias('q1'),
            F.first('median').alias('median'),
            F.first('q3').alias('q3'),
            F.min(F.when(F.col('valid_lower_whisker'), F.col(col_name))).alias('whisker_low'),
            F.max(F.when(F.col('valid_upper_whisker'), F.col(col_name))).alias('whisker_high')
        )
        .orderBy('pickup_hour')
    )

    return stat_df


def compute_hourly_medians_and_counts(df: DF, median_cols: list[str]) -> DF:
    """
    Calcula el número de registros y las medianas de varias métricas para cada combinación
    de día de la semana y hora de recogida.
    """
    required_columns = {
        'day_of_week',
        'pickup_hour',
        *median_cols
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f'Faltan columnas necesarias para la agregación temporal: {sorted(missing_columns)}'
        )
    # Las agregaciones de mediana se construyen dinámicamente
    # para reutilizar la función con distintas métricas
    median_aggregations = [
        F.median(F.col(col_name)).alias(col_name) for col_name in median_cols
    ]

    # Conteo y medianas se calculan en una única agregación distribuida y
    # las 24 horas se reorganizan mediante pivot
    stat_df = (
        df
        .groupBy('day_of_week')
        .pivot('pickup_hour', list(range(24)))
        .agg(F.count(F.lit(1)).alias('n_trips'), *median_aggregations)
    )

    return stat_df


# ============================================================
# FUNCIÓN DE RESUMEN ESTADÍSTICO
# ============================================================
def compute_comparison_stats(df, dataset_name: str):
    """
    Calcula estadísticos comparables de distancia y velocidad.
    """
    row = df.agg(
        F.count('*').alias('n_trips'),
        F.avg('trip_distance').alias('avg_distance'),
        F.expr('percentile_approx(trip_distance, 0.5, 10000)').alias('median_distance'),
        F.expr('percentile_approx(trip_distance, 0.95, 10000)').alias('p95_distance'),
        F.expr('percentile_approx(trip_distance, 0.99, 10000)').alias('p99_distance'),
        F.max('trip_distance').alias('max_distance'),
        F.avg('speed').alias('avg_speed'),
        F.expr('percentile_approx(speed, 0.5, 10000)').alias('median_speed'),
        F.expr('percentile_approx(speed, 0.95, 10000)').alias('p95_speed'),
        F.expr('percentile_approx(speed, 0.99, 10000)').alias('p99_speed'),
        F.max('speed').alias('max_speed')
    ).first()

    return {
        'Dataset': dataset_name,
        **row.asDict()
    }


def get_percentile_profile(
    df: DF, percentile_expression_1: str, alias_1: str, percentile_expression_2: str, alias_2: str
) -> tuple[list[float], list[float]]:
    """
    Calcula simultáneamente dos perfiles de percentiles definidos mediante
    expresiones de Spark SQL.

    Cada expresión debe devolver una colección de percentiles y se identifica
    mediante su alias correspondiente.
    """
    row = df.agg(
        F.expr(percentile_expression_1).alias(alias_1),
        F.expr(percentile_expression_2).alias(alias_2)
    ).first()

    return row[alias_1], row[alias_2]


def relative_change(original, new):
    """
    Calcula el cambio porcentual relativo entre un valor original
    y un nuevo valor.

    El valor original debe ser distinto de cero.
    """
    return 100 * (new - original) / original


# ============================================================
# FUNCIONES DE EXPORTACIÓN
# ============================================================
def save_figure(fig, filename: str, output_dir, dpi: int = 180) -> Path:
    """
    Guarda una figura de Matplotlib en formato PNG.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename

    if output_path.suffix == '':
        output_path = output_path.with_suffix('.png')

    fig.savefig(output_path, dpi=dpi, bbox_inches='tight', facecolor='white')

    return output_path


def save_table(df: pd.DataFrame, filename: str, output_dir, index: bool = True) -> Path:
    """
    Guarda un DataFrame de Pandas como CSV.

    La función acepta únicamente DataFrames de Pandas para evitar conversiones accidentales
    de grandes DataFrames de Spark al driver.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError('save_table solo acepta DataFrames de Pandas.')

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename

    if output_path.suffix == '':
        output_path = output_path.with_suffix('.csv')

    df.to_csv(output_path, index=index, encoding='utf-8')

    return output_path


# ============================================================
# FUNCIONES DE VISUALIZACIÓN
# ============================================================
def plot_community_districts_map(districts_gdf, trips_pd: pd.DataFrame) -> folium.Map:
    """
    Genera un mapa interactivo de los distritos comunales de Nueva York
    y una muestra de puntos de recogida.
    """
    required_trip_columns = {
        'pickup_latitude',
        'pickup_longitude'
    }

    if not required_trip_columns.issubset(trips_pd.columns):
        raise ValueError('trips_pd debe contener las columnas pickup_latitude y pickup_longitude.')

    if 'geometry' not in districts_gdf.columns:
        raise ValueError('districts_gdf debe contener una columna geometry.')

    geojson_dict = json.loads(districts_gdf.to_json())

    tooltip_fields = [
        column for column in ['BoroCD', 'district'] if column in districts_gdf.columns
    ]

    # Se descartan coordenadas nulas o iguales a cero
    valid_trips = trips_pd[
        trips_pd['pickup_latitude'].notna()
        & trips_pd['pickup_longitude'].notna()
        & (trips_pd['pickup_latitude'] != 0)
        & (trips_pd['pickup_longitude'] != 0)
    ]

    # El mapa se centra en la mediana de los pickups de la muestra
    if valid_trips.empty:
        map_center = [40.734695, -73.990372]

    else:
        map_center = [
            valid_trips['pickup_latitude'].median(), valid_trips['pickup_longitude'].median()
        ]

    nyc_map = folium.Map(
        location=map_center,
        tiles=None,
        zoom_start=14,
        control_scale=True,
    )

    nyc_map.get_root().header.add_child(
        folium.Element(
            """
            <style>.leaflet-container {
            background: #ffffff !important
            }
            </style>
            """
        )
    )

    # Los polígonos se representan sobre un fondo neutro
    folium.GeoJson(
        data=geojson_dict,
        name='NYC Community Districts',
        style_function=lambda feature: {
            'fillColor': '#f7f7f7',
            'color': '#666666',
            'weight': 0.8,
            'fillOpacity': 0.55
        },
        tooltip=(
            folium.GeoJsonTooltip(
                fields=tooltip_fields,
                labels=True
            )
            if tooltip_fields else None
        )
    ).add_to(nyc_map)

    # La muestra se utiliza únicamente como comprobación visual
    for row in valid_trips.itertuples(index=False):
        folium.CircleMarker(
            location=[row.pickup_latitude, row.pickup_longitude],
            radius=3,
            color='#2f5597',
            weight=0.8,
            fill=True,
            fill_color='#2f5597',
            fill_opacity=0.70,
        ).add_to(nyc_map)

    return nyc_map


def plot_boxplot_facets(stats_df, variable: str) -> tuple:
    """
    Representa los estadísticos de boxplot calculados en Spark para cada hora y día de la semana.

    El DataFrame debe contener:
    - pickup_hour;
    - {dia}_q1;
    - {dia}_median;
    - {dia}_q3;
    - {dia}_whisker_low;
    - {dia}_whisker_high.

    Se genera un panel por día de la semana y 24 boxplots dentro de cada panel, uno por hora.
    """
    # El resultado agregado es pequeño y puede visualizarse en Pandas
    if hasattr(stats_df, 'toPandas'):
        stats_pd = stats_df.toPandas()
    else:
        stats_pd = stats_df.copy()

    days = DAYS_ORDER

    required_columns = {'pickup_hour'}

    for day in days:
        required_columns.update({
            f'{day}_q1',
            f'{day}_median',
            f'{day}_q3',
            f'{day}_whisker_low',
            f'{day}_whisker_high'
        })

    missing_columns = required_columns - set(stats_pd.columns)

    if missing_columns:
        raise ValueError(f'Faltan columnas necesarias para los boxplots: {sorted(missing_columns)}')

    stats_pd = stats_pd.sort_values('pickup_hour').reset_index(drop=True)

    fig, axes = plt.subplots(
        nrows=7, ncols=1, figsize=(16, 24), sharex=True, sharey=True, squeeze=False
    )

    flat_axes = axes.flatten()

    # Se construye un panel independiente por cada día
    for day_index, day in enumerate(days):
        ax = flat_axes[day_index]

        box_stats = []
        positions = []

        for _, row in stats_pd.iterrows():
            hour = int(row['pickup_hour'])

            q1 = row[f'{day}_q1']
            median = row[f'{day}_median']
            q3 = row[f'{day}_q3']
            whisker_low = row[f'{day}_whisker_low']
            whisker_high = row[f'{day}_whisker_high']

            essential_values = [q1, median, q3]

            if any(pd.isna(value) for value in essential_values):
                continue

            box_stats.append({
                'med': float(median),
                'q1': float(q1),
                'q3': float(q3),
                'whislo': (float(whisker_low) if not pd.isna(whisker_low) else float(q1)),
                'whishi': (float(whisker_high) if not pd.isna(whisker_high) else float(q3)),
                'fliers': []
            })

            positions.append(hour)

        # bxp permite representar directamente los estadísticos calculados previamente en Spark
        # sin recuperar los datos originales
        ax.bxp(box_stats, positions=positions, showfliers=False, widths=0.6, manage_ticks=False)

        hour_ticks = list(range(0, 24, 2))

        ax.set_xticks(hour_ticks)
        ax.set_xticklabels([str(hour) for hour in hour_ticks])
        ax.set_xlim(-1, 24)

        if day_index == len(days) - 1:
            ax.set_xlabel('Hora de recogida')
        else:
            ax.set_xlabel('')

        ax.set_ylabel(variable)
        ax.set_title(DAY_LABELS.get(day, day.capitalize()))

        ax.grid(axis='y', alpha=0.25)

    fig.suptitle(f'Distribución de {variable} por hora y día de la semana', fontsize=15)

    fig.tight_layout(rect=[0, 0, 1, 0.97])

    return fig, axes


def plot_week_hour_heatmap(
    df_pd: pd.DataFrame, title: str, cbar_label: str, cmap: str = 'viridis', fmt: str = '.1f',
    annot: bool = True, figsize: tuple = (14, 4)
) -> tuple:
    """
    Representa una matriz día × hora mediante un heatmap.
    """
    if 'day_of_week' not in df_pd.columns:
        raise ValueError('El DataFrame debe contener la columna day_of_week.')

    heatmap_data = df_pd.copy().set_index('day_of_week', drop=True)
    heatmap_data.index = [DAY_LABELS.get(day, day) for day in heatmap_data.index]
    heatmap_data.columns = [int(column) for column in heatmap_data.columns]
    heatmap_data = heatmap_data[sorted(heatmap_data.columns)]

    fig, ax = plt.subplots(figsize=figsize)

    sns.heatmap(
        heatmap_data, ax=ax, cmap=cmap, linewidths=0.4, linecolor='white', annot=annot,
        fmt=fmt, annot_kws={'size': 7}, cbar_kws={'label': cbar_label}
    )

    ax.set_title(title, fontsize=14, pad=12)
    ax.set_xlabel('Hora del día', fontsize=11)
    ax.set_ylabel('Día de la semana', fontsize=11)

    ax.tick_params(axis='x', rotation=0)
    ax.tick_params(axis='y', rotation=0)

    fig.tight_layout()

    return fig, ax


def plot_origin_destination_heatmap(
    od_matrix_pd: pd.DataFrame, title: str = 'Flujos de viajes entre distritos',
    normalize: bool = False, figsize: tuple = (15, 12)
) -> tuple:
    """
    Representa una matriz origen-destino mediante un heatmap.

    Puede mostrar:
        - número absoluto de viajes;
        - porcentaje de destinos dentro de cada distrito de origen.
    """
    if od_matrix_pd.empty:
        raise ValueError('La matriz origen-destino está vacía.')

    plot_data = od_matrix_pd.copy()

    if normalize:
        row_totals = plot_data.sum(axis=1).replace(0, pd.NA)
        plot_data = plot_data.div(row_totals, axis=0).mul(100).fillna(0)

        cmap = 'viridis'
        annotations = plot_data.map(lambda value: f'{value:.1f}')
        cbar_label = '% entre destinos seleccionados'

    else:
        cmap = 'YlOrRd'
        annotations = plot_data.map(
            lambda value: (f'{value / 1_000:.0f}k' if value >= 1_000 else f'{value:.0f}')
        )
        cbar_label = 'Número de viajes'


    fig, ax = plt.subplots(figsize=figsize)

    sns.heatmap(
        plot_data, ax=ax, cmap=cmap, linewidths=0.3, linecolor='white', annot=annotations,
        fmt='', annot_kws={'size': 8}, cbar_kws={'label': cbar_label}
    )

    ax.set_title(title, fontsize=14, pad=12)
    ax.set_xlabel('Distrito de destino', fontsize=11)
    ax.set_ylabel('Distrito de origen', fontsize=11)

    ax.tick_params(axis='x', rotation=90)
    ax.tick_params(axis='y', rotation=0)

    fig.tight_layout()

    return fig, ax