# Usamos Python 3.12 (slim para que sea ligero)
FROM python:3.12-slim

# Evita que Python genere archivos .pyc y fuerza salida en consola en tiempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# 1. Copiar dependencias e instalarlas (Capa de caché)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copiar el resto del código
# Nota: Copiamos las carpetas scripts, raw y sql (esta última para asegurar que exista)
COPY scripts/ ./scripts/
COPY raw/ ./raw/
# Creamos la carpeta sql por si no existe, para montar el volumen ahí
RUN mkdir -p sql

# 3. Comando por defecto: Ejecutar los 3 scripts en secuencia
CMD ["sh", "-c", "python scripts/normalize_netflix.py && python scripts/normalize_ecommerce.py && python scripts/normalize_hospital.py"]