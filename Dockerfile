# Stage 1: "builder" - Instala las dependencias en un entorno robusto
FROM python:3.11-bullseye as builder

# Instala las dependencias del sistema necesarias para compilar algunas librerías de Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev

# Crea un entorno de trabajo y un entorno virtual
WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copia e instala los requerimientos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: "final" - Construye la imagen de producción ligera
FROM python:3.11-slim-bullseye

# Instala SOLO las librerías de sistema necesarias para EJECUTAR la aplicación
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia el entorno virtual con las dependencias desde la etapa "builder"
COPY --from=builder /opt/venv /opt/venv

# Copia el código de tu aplicación
COPY . .

# Activa el entorno virtual para los comandos que se ejecuten
ENV PATH="/opt/venv/bin:$PATH"

# Expone el puerto que la aplicación usará internamente
EXPOSE 8000

# El comando por defecto. Tu "Start Command" en la plataforma de deploy lo sobreescribirá.
CMD ["python", "postgres_server.py"]
