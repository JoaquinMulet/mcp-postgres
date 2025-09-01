# Usa una versión de Python reciente y 'slim' para una imagen más pequeña.
FROM python:3.11-slim

# Establece el directorio de trabajo dentro del contenedor.
WORKDIR /app

# --- OPTIMIZACIÓN DE CACHÉ ---
# 1. Copia SOLO el archivo de requerimientos primero.
COPY requirements.txt .

# 2. Instala las dependencias.
# Docker creará una capa aquí. Si requirements.txt no cambia,
# esta capa se reutilizará en futuras construcciones, ahorrando mucho tiempo.
RUN pip install --no-cache-dir -r requirements.txt

# 3. Ahora copia el resto del código de tu aplicación.
# Como el código cambia más a menudo, solo esta capa se reconstruirá.
COPY . .

# Expone el puerto en el que la aplicación escuchará.
EXPOSE 8000

# Define el comando por defecto para iniciar el servidor.
# El "Start Command" de Railway puede sobreescribir esto.
CMD ["python", "postgre_server.py", "--host", "0.0.0.0"]
