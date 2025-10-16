# ---- Build base ----
FROM node:20-slim

# Instalar ffmpeg y certificados
RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Directorio de la app
WORKDIR /app

# Copiar manifiestos y instalar deps (sin dev)
COPY package*.json ./
RUN npm install --omit=dev

# Copiar el resto del código
COPY . .

# Exponer puerto
EXPOSE 3000

# Arranque
CMD ["npm", "start"]
