import os
import pandas as pd
import utils  # El nuevo módulo unificado

def main():
    # 1. Configurar Rutas
    # CAMBIO: Usamos el diccionario que devuelve la nueva función
    paths = utils.get_project_paths(__file__)
    input_file = os.path.join(paths['raw'], 'netflix_titles.csv')
    output_file = os.path.join(paths['sql'], 'netflix_normalized.sql')
    
    print("--- Procesando Dataset: Netflix ---")
    
    # 2. Cargar Datos
    # CAMBIO: load_csv ahora es load_data
    df = utils.load_data(input_file)
    if df is None: return

    # Limpieza específica de este dataset
    text_cols = ['director', 'cast', 'country', 'rating', 'date_added', 'duration']
    df = utils.clean_text_columns(df, text_cols)
    df['release_year'] = df['release_year'].fillna(0).astype(int)

    # 3. Normalización
    print("Generando catálogos y relaciones...")
    actors_cat, actors_rel = utils.create_catalog(df, 'cast', 'actor_id')
    directors_cat, directors_rel = utils.create_catalog(df, 'director', 'director_id')
    countries_cat, countries_rel = utils.create_catalog(df, 'country', 'country_id')
    genres_cat, genres_rel = utils.create_catalog(df, 'listed_in', 'genre_id')

    cols_drop = ['cast', 'director', 'country', 'listed_in']
    shows_df = df.drop(columns=cols_drop).drop_duplicates('show_id')

    # 4. Generación de SQL
    print("Construyendo script SQL...")
    
    # Creamos una lista para ir acumulando el contenido (más eficiente que concatenar strings gigantes)
    sql_parts = ["-- Script Generado para Netflix (3FN)\nBEGIN;\n\n"]
    
    # DDL Manual
    sql_parts.append("""
    CREATE TABLE IF NOT EXISTS actors (actor_id SERIAL PRIMARY KEY, name VARCHAR(500));
    CREATE TABLE shows (show_id VARCHAR(20) PRIMARY KEY, type VARCHAR(50), title VARCHAR(500), date_added VARCHAR(100), release_year INT, rating VARCHAR(50), duration VARCHAR(100), description TEXT);
    CREATE TABLE IF NOT EXISTS shows_actors (show_id VARCHAR(20), actor_id INT, PRIMARY KEY(show_id, actor_id));
    CREATE TABLE IF NOT EXISTS directors (director_id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL);
    CREATE TABLE IF NOT EXISTS countries (country_id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL);
    CREATE TABLE IF NOT EXISTS genres (genre_id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL);
    CREATE TABLE IF NOT EXISTS shows_directors (show_id VARCHAR(20) REFERENCES shows(show_id), director_id INT REFERENCES directors(director_id), PRIMARY KEY (show_id, director_id));
    CREATE TABLE IF NOT EXISTS shows_countries (show_id VARCHAR(20) REFERENCES shows(show_id), country_id INT REFERENCES countries(country_id), PRIMARY KEY (show_id, country_id));
    CREATE TABLE IF NOT EXISTS shows_genres (show_id VARCHAR(20) REFERENCES shows(show_id), genre_id INT REFERENCES genres(genre_id), PRIMARY KEY (show_id, genre_id));
	""")

    # DML (Inserts) usando la nueva utilidad df_to_sql_insert
    sql_parts.append(utils.df_to_sql_insert(actors_cat, 'actors'))
    sql_parts.append(utils.df_to_sql_insert(shows_df, 'shows'))
    sql_parts.append(utils.df_to_sql_insert(actors_rel, 'shows_actors'))
    sql_parts.append(utils.df_to_sql_insert(directors_cat, 'directors'))
    sql_parts.append(utils.df_to_sql_insert(countries_cat, 'countries'))
    sql_parts.append(utils.df_to_sql_insert(genres_cat, 'genres'))
    sql_parts.append(utils.df_to_sql_insert(directors_rel, 'shows_directors'))
    sql_parts.append(utils.df_to_sql_insert(countries_rel, 'shows_countries'))
    sql_parts.append(utils.df_to_sql_insert(genres_rel, 'shows_genres'))
    # ... agregar los demás dataframes ...

    sql_parts.append("\nCOMMIT;")
    
    # 5. Guardar usando la función que acepta listas
    utils.write_sql_file(sql_parts, output_file)

if __name__ == "__main__":
    main()
