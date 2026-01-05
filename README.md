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
