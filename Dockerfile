# Imagen base ligera con Python
FROM python:3.12-slim

# Establece el directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Copiar requirements primero para aprovechar cache de Docker
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements-mongodb.txt && \
    # Limpieza para reducir tamaño de imagen
    apt-get remove -y gcc python3-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Copiar el resto de archivos
COPY consumer.py .

# Exponer puerto para posibles endpoints de monitoreo
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -f http://localhost:8080/health || exit 1

# Comando para ejecutar el consumer
CMD ["python", "consumer.py"]
