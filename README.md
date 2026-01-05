# 🗄️ Automatización de Normalización de Bases de Datos (ETL con Docker)

![Python](https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-ETL-150458?style=for-the-badge&logo=pandas&logoColor=white)

Este proyecto implementa un pipeline automatizado de **ETL (Extract, Transform, Load)** para normalizar conjuntos de datos "crudos" (CSV) hasta la **Tercera Forma Normal (3FN)**.

El sistema procesa archivos desnormalizados, limpia los datos, separa entidades, genera esquemas SQL relacionales y exporta los resultados, todo orquestado dentro de un entorno contenerizado con **Docker**.

## 📋 Descripción del Proyecto

El objetivo es resolver problemas comunes en bases de datos desnormalizadas (redundancia, anomalías de inserción/borrado) mediante scripts de Python que transforman los datos automáticamente.

### Datasets Procesados
El sistema maneja tres escenarios de datos reales obtenidos de Kaggle:
1.  **Netflix Movies & TV Shows:** Solución a listas multivaluadas (Actores, Géneros) -> **1FN**.
2.  **E-commerce Sales:** Eliminación de redundancia transaccional -> **2FN**.
3.  **Hospital Patient Records:** Separación de dependencias transitivas (Doctores, Pacientes) -> **3FN**.

## 🚀 Arquitectura y Tecnologías

El proyecto utiliza una arquitectura de microservicios con Docker Compose:

* **App (Python 3.12):** Ejecuta scripts con `pandas` para limpieza y normalización. Genera archivos `.sql` (DDL/DML) y `.csv` limpios.
* **Database (PostgreSQL 15):** Servidor de base de datos que inicializa automáticamente tres bases de datos independientes (`netflix`, `ecommerce`, `hospital`).
* **Interfaz (pgAdmin 4):** Cliente web para visualizar y administrar los datos.

## 📂 Estructura del Repositorio

```text
normalizacion-db/
├── docker-compose.yml      # Orquestación de servicios
├── Dockerfile              # Definición de imagen de Python ETL
├── requirements.txt        # Dependencias (pandas, numpy)
├── raw/                    # [ENTRADA] Archivos CSV originales (Fuente)
├── sql/                    # [SALIDA] Scripts SQL generados automáticamente
├── data/
│   └── normalized/         # [SALIDA] Archivos CSV exportados por tabla
├── scripts/                # Lógica de Normalización (Código Fuente)
│   ├── utils.py            # Funciones reutilizables (DRY)
│   ├── normalize_netflix.py
│   ├── normalize_ecommerce.py
│   └── normalize_hospital.py
└── docker-entrypoint/      # Scripts de inicialización de BD
```
## 🛠️ Instalación y Uso

Prerrequisitos:
-Docker Desktop instalado y ejecutándose.
-(Opcional) Git para clonar el repositorio.

Paso 1: Descargar el archivo .zip y descomprimir
Nota: Asegúrate de que los archivos .csv originales estén en la carpeta raw/.

Paso 2: Ejecutar con Docker
Levanta todo el entorno con un solo comando. Docker construirá la imagen de Python, instalará las librerías y ejecutará los scripts.
```bash
docker compose up --build
```
Paso 3: Verificar Resultados
Una vez que la terminal muestre que los scripts han finalizado, puedes verificar:
Archivos SQL: Revisa la carpeta sql/ para ver el código generado.
CSVs Normalizados: Revisa la carpeta data/normalized/ para ver las tablas separadas.
Base de Datos: Accede a pgAdmin 4 desde tu navegador

## 🖥️ Acceso a Servicios
Servicio,URL / Dirección,Credenciales
pgAdmin 4 (Web),http://localhost:5050,Email: admin@admin.com  Pass: root
PostgreSQL (Externo),localhost:5433,User: admin_user  Pass: admin_password
PostgreSQL (Interno),Host: db Port: 5432,(Para configurar dentro de pgAdmin)

Nota: El puerto externo de PostgreSQL se configuró en 5433 para evitar conflictos con instalaciones locales en tu máquina.

## 🧠 Metodología de Normalización
El script de Python aplica las siguientes reglas teóricas:
1FN (Atomicidad): Se identifican columnas con listas (ej. "Actor A, Actor B") y se utiliza explode() de Pandas para separar en registros únicos.
2FN (Dependencias Parciales): Se separan atributos que no dependen de la clave completa en tablas maestras (ej. Tabla Shows separada de Shows_Actors).
3FN (Dependencias Transitivas): Se crean catálogos independientes (ej. Countries, Genres) y se referencian mediante claves foráneas (IDs) para eliminar redundancia de texto.

## Hecho con 🐍 y ❤️ para la clase de Bases de Datos.
