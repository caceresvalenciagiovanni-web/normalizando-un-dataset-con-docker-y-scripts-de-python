import os
import pandas as pd
import utils  # El nuevo módulo unificado

# --- Lógica Específica del Dataset 2 ---

def clean_ecommerce_specific(df):
    """Limpieza específica para columnas de E-commerce"""
    # Verificamos si existen las columnas antes de operar para evitar errores
    if 'CustomerID' in df.columns:
        df['CustomerID'] = df['CustomerID'].fillna(0).astype(int)
    if 'Description' in df.columns:
        df['Description'] = df['Description'].fillna('N/A')
    if 'Country' in df.columns:
        df['Country'] = df['Country'].fillna('Unknown')
    return df

def normalize_ecommerce(df):
    # 1FN: Separar fechas
    if 'InvoiceDate' in df.columns:
        # Convertir a datetime, manejando errores
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
        df['Date'] = df['InvoiceDate'].dt.date
        df['Time'] = df['InvoiceDate'].dt.time
        df = df.drop(columns=['InvoiceDate'])
    
    df = df.drop_duplicates()

    # 3FN: Creación de tablas independientes
    # Usamos .copy() para evitar advertencias de SettingWithCopyWarning
    customers = df[['CustomerID', 'Country']].drop_duplicates('CustomerID').copy()
    products = df[['StockCode', 'Description']].drop_duplicates('StockCode').copy()
    invoices = df[['InvoiceNo', 'CustomerID', 'Date', 'Time']].drop_duplicates('InvoiceNo').copy()
    
    details = df.groupby(['InvoiceNo', 'StockCode']).agg({
        'Quantity': 'sum',
        'UnitPrice': 'mean'
    }).reset_index()

    return {
        'Customers': customers,
        'Products': products,
        'Invoices': invoices,
        'Order_Details': details
    }

def get_ddl_ecommerce():
    """Devuelve el string con la creación de tablas"""
    return """
    CREATE TABLE IF NOT EXISTS Customers (CustomerID INT PRIMARY KEY, Country VARCHAR(100));
    CREATE TABLE IF NOT EXISTS Products (StockCode VARCHAR(50) PRIMARY KEY, Description TEXT);
    CREATE TABLE IF NOT EXISTS Invoices (InvoiceNo VARCHAR(50) PRIMARY KEY, CustomerID INT, Date DATE, Time TIME);
    CREATE TABLE IF NOT EXISTS Order_Details (InvoiceNo VARCHAR(50), StockCode VARCHAR(50), Quantity INT, UnitPrice DECIMAL(10,2), PRIMARY KEY(InvoiceNo, StockCode));
    """

# --- Ejecución Principal ---
def main():
    # 1. Configurar Rutas (Adaptado al nuevo utils)
    paths = utils.get_project_paths(__file__)
    # Nota: Asegúrate de que el archivo CSV se llame así en tu carpeta 'raw'
    input_csv = os.path.join(paths['raw'], 'data.csv') 
    output_sql = os.path.join(paths['sql'], 'ecommerce_normalized.sql')
    
    print("--- Procesando Dataset: E-commerce ---")

    # 2. Cargar (Adaptado al nuevo utils)
    df_raw = utils.load_data(input_csv)
    if df_raw is None: return
    
    # 3. Normalizar
    df_clean = clean_ecommerce_specific(df_raw)
    normalized_tables = normalize_ecommerce(df_clean)
    
    # 4. Generar Contenido SQL
    print("Construyendo script SQL...")
    sql_parts = ["-- Script E-commerce\nBEGIN;\n"]
    
    # Agregar DDL
    sql_parts.append(get_ddl_ecommerce())
    
    # Agregar Inserts (Iteramos sobre el diccionario de tablas devuelto)
    for table_name, df_table in normalized_tables.items():
        print(f"Generando inserts para: {table_name}...")
        sql_parts.append(utils.df_to_sql_insert(df_table, table_name))
        
    sql_parts.append("\nCOMMIT;")

    # 5. Guardar Archivo
    utils.write_sql_file(sql_parts, output_sql)

if __name__ == "__main__":
    main()
