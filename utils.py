import pandas as pd
import os
import datetime

# --- SECCIÓN 1: GESTIÓN DE RUTAS ---

def get_project_paths(script_path):
    """
    Calcula las rutas absolutas base del proyecto.
    Devuelve un diccionario con las rutas a las carpetas clave.
    """
    # Carpeta del script actual (scripts/)
    script_dir = os.path.dirname(os.path.abspath(script_path))
    # Carpeta raíz del proyecto (subir un nivel)
    base_dir = os.path.dirname(script_dir)
    
    paths = {
        'root': base_dir,
        'raw': os.path.join(base_dir, 'raw'),
        'sql': os.path.join(base_dir, 'sql')
    }
    return paths

# --- SECCIÓN 2: CARGA Y LIMPIEZA DE DATOS ---

def load_data(filepath):
    """
    Carga robusta de CSV. Intenta UTF-8 primero, si falla usa ISO-8859-1.
    Maneja errores si el archivo no existe.
    """
    if not os.path.exists(filepath):
        print(f"ERROR: No se encontró el archivo en: {filepath}")
        return None
        
    try:
        return pd.read_csv(filepath, encoding='utf-8')
    except UnicodeDecodeError:
        print(f"Advertencia: UTF-8 falló para {filepath}, intentando con ISO-8859-1")
        return pd.read_csv(filepath, encoding='ISO-8859-1')

def clean_text_columns(df, columns):
    """
    Rellena valores nulos en columnas de texto especificadas con 'N/A'.
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].fillna('N/A')
    return df

def create_catalog(df, col_name, id_col_name):
    """
    Normaliza una columna que contiene listas (ej. "Acción, Drama") en un catálogo separado.
    Retorna:
        1. catalog_df: DataFrame con IDs únicos y nombres.
        2. relation_df: Tabla intermedia para relación muchos-a-muchos.
    """
    # 1. Copia y limpieza básica
    temp_df = df[['show_id', col_name]].copy()
    # Asegurar que no sea nulo y convertir a string para evitar errores en el split
    temp_df = temp_df[temp_df[col_name].notna() & (temp_df[col_name] != 'N/A')]
    
    # 2. Separar listas (Atomicidad)
    temp_df[col_name] = temp_df[col_name].astype(str).apply(lambda x: x.split(', '))
    exploded_df = temp_df.explode(col_name)
    exploded_df[col_name] = exploded_df[col_name].str.strip()
    
    # 3. Crear catálogo único
    unique_vals = exploded_df[col_name].unique()
    catalog_df = pd.DataFrame(unique_vals, columns=['name']).sort_values('name')
    # Crear ID numérico
    catalog_df[id_col_name] = range(1, len(catalog_df) + 1)
    
    # 4. Crear relación (Tabla pivote)
    relation_df = exploded_df.merge(catalog_df, left_on=col_name, right_on='name')
    relation_df = relation_df[['show_id', id_col_name]]
    
    return catalog_df[[id_col_name, 'name']], relation_df

# --- SECCIÓN 3: GENERACIÓN DE SQL ---

def df_to_sql_insert(df, table_name):
    """
    Convierte un DataFrame en una cadena larga de sentencias INSERT INTO.
    Maneja Strings, Fechas, Números y Nulos.
    """
    statements = []
    
    # Cabecera para legibilidad
    statements.append(f"\n-- Datos para tabla: {table_name} --")
    
    for _, row in df.iterrows():
        vals = []
        for v in row:
            if pd.isna(v):
                vals.append("NULL")
            elif isinstance(v, str):
                # Escapar comillas simples (O'Reilly -> O''Reilly)
                clean_str = v.replace("'", "''")
                vals.append(f"'{clean_str}'")
            elif isinstance(v, (datetime.date, datetime.time, pd.Timestamp)):
                vals.append(f"'{v}'")
            else:
                vals.append(str(v))
        
        val_str = ", ".join(vals)
        # Usamos ON CONFLICT DO NOTHING para evitar errores si se corre dos veces (opcional)
        statements.append(f"INSERT INTO {table_name} VALUES ({val_str}) ON CONFLICT DO NOTHING;")
    
    return "\n".join(statements)

def write_sql_file(content_list, output_path):
    """
    Escribe una lista de contenidos (strings) en un archivo SQL.
    """
    print(f"\n--- Generando script SQL en: {output_path} ---")
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("-- Script generado automáticamente\n")
            f.write("-- Base de Datos: PostgreSQL\n\n")
            
            # Si content_list es una lista, la unimos; si es string, lo escribimos directo
            if isinstance(content_list, list):
                f.write("\n".join(content_list))
            else:
                f.write(content_list)
                
        print("¡SQL Generado exitosamente!")
    except Exception as e:
        print(f"Error escribiendo archivo: {e}")
