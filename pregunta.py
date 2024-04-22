"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""





import pandas as pd

import pandas as pd
def load_data(filename):
    df = pd.read_fwf(filename,
                     skiprows=4,
                     index_col=False,
                     widths=[7,6,18,87],
                     header=None,
                     names=['cluster',
                            'cantidad_de_palabras_clave',
                            'porcentaje_de_palabras_clave',
                            'principales_palabras_clave'],
                            decimal=".",
                            )
    return df


def procesamiento_datos(df):
    
    df["cluster"] = df["cluster"].ffill()
    df["principales_palabras_clave"] = df.groupby(["cluster"])["principales_palabras_clave"
    ].transform(lambda x: " ".join(x))
    df = df.drop_duplicates(subset=["cluster"])
    df = df.reset_index(drop=True)
    df["cluster"] = df["cluster"].astype(int)
   
   
    # Eliminar el porcentaje completamente de los valores
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].str.replace("%", "")
    # Eliminar el porcentaje completamente de los valores
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].str.replace(",", ".")
    # Convertir la columna 'porcentaje_de_palabras_clave' a números flotantes
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].astype(float)
    
    df["principales_palabras_clave"] = df["principales_palabras_clave"].str.strip(
        "."
    )
    df["principales_palabras_clave"] = df["principales_palabras_clave"].replace(
        r"\s+", " ", regex=True
    )
    return df


def ingest_data():
    df = load_data("clusters_report.txt")
    df = procesamiento_datos(df)
    return df



