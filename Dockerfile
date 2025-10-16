FROM node:20-slim

# FFmpeg + certificados
RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalar dependencias sin dev (no requiere package-lock.json)
COPY package*.json ./
RUN npm install --omit=dev

# Copiar código
COPY . .

EXPOSE 3000
CMD ["npm", "start"]
