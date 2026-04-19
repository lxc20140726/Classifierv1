FROM node:20-alpine AS frontend-builder

WORKDIR /src/frontend

COPY frontend/package*.json ./
RUN npm ci

COPY frontend/ ./
RUN npm run build

FROM golang:1.26-alpine AS backend-builder

WORKDIR /src/backend

COPY backend/go.mod backend/go.sum ./
RUN go mod download

COPY backend/ ./
COPY --from=frontend-builder /src/frontend/dist ./cmd/server/web/dist

ENV CGO_ENABLED=0
RUN go build -o /out/classifier ./cmd/server

FROM alpine:3.20

RUN apk add --no-cache ffmpeg ca-certificates tzdata

WORKDIR /app

COPY --from=backend-builder /out/classifier /app/classifier

ENV CONFIG_DIR=/config
ENV TMP_WORK_DIR=/tmp/work

EXPOSE 8080

ENTRYPOINT ["/app/classifier"]
