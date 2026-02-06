FROM node:22-alpine AS builder
RUN apk add --no-cache python3 make g++
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci
COPY tsconfig.json ./
COPY src/ ./src/
RUN npm run build

FROM node:22-alpine
RUN apk add --no-cache python3 make g++ wget
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci --only=production && rm -rf /root/.npm
COPY --from=builder /app/dist ./dist
EXPOSE 3214
CMD ["node", "dist/server.js"]
