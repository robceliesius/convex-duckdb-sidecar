FROM node:22-bookworm-slim AS builder
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 make g++ \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci
COPY tsconfig.json ./
COPY src/ ./src/
RUN npm run build

FROM node:22-bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 make g++ wget ca-certificates \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV SIDECAR_MAX_CONCURRENT_SNAPSHOTS=4
ENV SIDECAR_MAX_SNAPSHOT_QUEUE=100
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev && rm -rf /root/.npm
COPY --from=builder /app/dist ./dist
EXPOSE 3214
CMD ["node", "dist/server.js"]
