import pandas as pd
import numpy as np
import re
import os

# --- FUNCIONES DE PROCESAMIENTO ---

def limpiar_nombre_columna(nombre):
    """
    Limpia nombres de columnas para que sean compatibles con SQL.
    Ej: "Unnamed: 83" -> "unnamed_83" (aunque las borraremos antes)
    Ej: "Total (kg)" -> "total_kg"
    """
    # Reemplazar caracteres no alfanuméricos por guion bajo
    nuevo_nombre = re.sub(r'\W+', '_', nombre).lower()
    # Eliminar guiones bajos al inicio o final
    nuevo_nombre = nuevo_nombre.strip('_')
    return nuevo_nombre

def cargar_y_limpiar(filepath):
    print(f"--- 1. Buscando archivo en: {filepath} ---")
    try:
        if not os.path.exists(filepath):
            print(f"ERROR: No se encuentra el archivo en la ruta especificada.")
            return None
            
        df = pd.read_csv(filepath)
        print(f"Datos cargados: {df.shape[0]} filas, {df.shape[1]} columnas.")
        
        # --- NUEVO: Eliminar columnas basura (Unnamed) ---
        cols_basura = [c for c in df.columns if 'Unnamed' in c]
        if cols_basura:
            print(f"Eliminando {len(cols_basura)} columnas basura: {cols_basura}")
            df.drop(columns=cols_basura, inplace=True)

        # Imputación
        cols_num = df.select_dtypes(include=np.number).columns
        df[cols_num] = df[cols_num].fillna(0)
        cols_obj = df.select_dtypes(include='object').columns
        df[cols_obj] = df[cols_obj].fillna('N/A')
        
        return df
    except Exception as e:
        print(f"Error crítico al cargar el archivo: {e}")
        return None

def normalizar_1fn(df):
    print("\n--- 2. Aplicando Primera Forma Normal (1FN) ---")
    patron_medicion = re.compile(r'^(d1|h1)_')
    cols_medicion = [c for c in df.columns if patron_medicion.match(c)]
    cols_base = [c for c in df.columns if c not in cols_medicion]
    
    # Unpivot
    df_mediciones = df.melt(
        id_vars=['encounter_id'], 
        value_vars=cols_medicion,
        var_name='metric_name', 
        value_name='metric_value'
    )
    df_encuentros_base = df[cols_base].copy()
    print("Tablas separadas correctamente (Base y Mediciones).")
    return df_encuentros_base, df_mediciones

def normalizar_2fn_3fn(df_base):
    print("\n--- 3. Aplicando Segunda (2FN) y Tercera Forma Normal (3FN) ---")
    
    # 2FN: Pacientes
    cols_paciente = ['patient_id', 'gender', 'ethnicity']
    df_pacientes = df_base[cols_paciente].drop_duplicates().reset_index(drop=True)
    df_pacientes = df_pacientes.drop_duplicates(subset=['patient_id'], keep='last')

    # 3FN: UCIs
    cols_uci = ['icu_id', 'icu_type', 'hospital_id', 'icu_admit_source']
    # Aseguramos que existan las columnas antes de seleccionar (por si acaso se borró alguna)
    cols_uci_existentes = [c for c in cols_uci if c in df_base.columns]
    df_uci = df_base[cols_uci_existentes].drop_duplicates().reset_index(drop=True)
    df_uci = df_uci.drop_duplicates(subset=['icu_id'], keep='first')

    # 3FN: Diagnósticos
    cols_diag = ['apache_3j_diagnosis', 'apache_3j_bodysystem']
    cols_diag_existentes = [c for c in cols_diag if c in df_base.columns]
    df_diagnosticos = df_base[cols_diag_existentes].drop_duplicates().reset_index(drop=True)
    # Renombrar solo si existen ambas columnas
    if len(df_diagnosticos.columns) == 2:
        df_diagnosticos.columns = ['diagnosis_code', 'body_system']
    
    if 'diagnosis_code' in df_diagnosticos.columns:
        df_diagnosticos = df_diagnosticos.drop_duplicates(subset=['diagnosis_code'], keep='first')

    # Limpieza final de Encuentros
    cols_eliminar = ['gender', 'ethnicity', 'icu_type', 'hospital_id', 
                     'icu_admit_source', 'apache_3j_bodysystem', 'bmi']
    cols_a_borrar = [c for c in cols_eliminar if c in df_base.columns]
    df_encuentros_final = df_base.drop(columns=cols_a_borrar)
    
    return {
        'pacientes': df_pacientes,
        'unidades_uci': df_uci,
        'diagnosticos': df_diagnosticos,
        'encuentros': df_encuentros_final
    }

def generar_sql(diccionario_tablas, df_mediciones, output_path):
    print(f"\n--- 4. Generando Script SQL en: {output_path} ---")
    
    def map_dtype(dtype):
        if pd.api.types.is_integer_dtype(dtype): return 'INTEGER'
        if pd.api.types.is_float_dtype(dtype): return 'FLOAT'
        return 'VARCHAR(255)'

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("BEGIN;\n\n")
            
            orden = [
                ('pacientes', diccionario_tablas['pacientes'], 'patient_id'),
                ('unidades_uci', diccionario_tablas['unidades_uci'], 'icu_id'),
                ('diagnosticos', diccionario_tablas['diagnosticos'], 'diagnosis_code'),
                ('encuentros', diccionario_tablas['encuentros'], 'encounter_id'),
                ('mediciones', df_mediciones.head(1000), None) 
            ]

            for nombre, df, pk in orden:
                # --- NUEVO: Sanitizar nombres de columnas ---
                # Creamos un mapa de nombre_viejo -> nombre_nuevo_limpio
                cols_map = {col: limpiar_nombre_columna(col) for col in df.columns}
                
                # DDL
                cols_def = []
                for col_orig in df.columns:
                    col_sql = cols_map[col_orig] # Usamos el nombre limpio
                    tipo = map_dtype(df[col_orig].dtype)
                    
                    # Definición de columnas
                    if col_orig == pk: 
                        cols_def.append(f"    {col_sql} {tipo} PRIMARY KEY")
                    else: 
                        cols_def.append(f"    {col_sql} {tipo}")
                
                # Definición de FKs (usando nombres limpios)
                if nombre == 'encuentros':
                    cols_def.append("    FOREIGN KEY (patient_id) REFERENCES pacientes(patient_id)")
                    cols_def.append("    FOREIGN KEY (icu_id) REFERENCES unidades_uci(icu_id)")
                    # Nota: diagnosis podría requerir ajuste manual si diagnosis_code es float
                
                if nombre == 'mediciones':
                    cols_def.append("    FOREIGN KEY (encounter_id) REFERENCES encuentros(encounter_id)")

                f.write(f"CREATE TABLE IF NOT EXISTS {nombre} (\n" + ",\n".join(cols_def) + "\n);\n")
                
                # DML
                print(f"Generando INSERTS para {nombre}...")
                for _, row in df.iterrows():
                    vals = []
                    for val in row:
                        if isinstance(val, str):
                            vals.append(f"'{val.replace("'", "''")}'")
                        elif pd.isna(val) or val == 'nan':
                            vals.append("NULL")
                        else:
                            vals.append(str(val))
                    
                    # Usamos los nombres limpios en el INSERT
                    cols_limpias = [cols_map[c] for c in df.columns]
                    f.write(f"INSERT INTO {nombre} ({', '.join(cols_limpias)}) VALUES ({', '.join(vals)}) ON CONFLICT DO NOTHING;\n")
                f.write("\n")
                
            f.write("COMMIT;\n")
        print("¡Archivo SQL generado exitosamente y sanitizado!")
    except Exception as e:
        print(f"Error al escribir el archivo SQL: {e}")

# --- BLOQUE PRINCIPAL ---
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ruta_input = os.path.join(base_dir, '..', 'raw', 'dataset.csv')
    ruta_output = os.path.join(base_dir, '..', 'sql', 'hospital_normalizado.sql')
    
    ruta_input = os.path.normpath(ruta_input)
    ruta_output = os.path.normpath(ruta_output)

    print(f"Directorio base del script: {base_dir}")
    
    # Ejecución
    df_raw = cargar_y_limpiar(ruta_input)
    
    if df_raw is not None:
        df_base, df_mediciones = normalizar_1fn(df_raw)
        tablas_maestras = normalizar_2fn_3fn(df_base)
        generar_sql(tablas_maestras, df_mediciones, ruta_output)
