# Imagen base con Node
FROM node:20-slim

# Instala ffmpeg y certificados
RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Directorio de trabajo
WORKDIR /app

# Copia package.json y package-lock.json
COPY package*.json ./

# Instala dependencias en modo producción
RUN npm ci --only=production

# Copia el resto del código
COPY . .

# Variables y puerto
ENV NODE_ENV=production
ENV PORT=8080
EXPOSE 8080

# Importante: ejecuta Node como proceso principal (sin ENTRYPOINT de ffmpeg)
CMD ["node", "server.js"]
