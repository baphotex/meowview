# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o meow-app .

# Final stage
FROM alpine:3.18

WORKDIR /app

# Install wait-for-it.sh and SSL certificates
RUN apk add --no-cache bash ca-certificates

# Copy wait-for-it script
COPY --from=builder /app/wait-for-it.sh /app/wait-for-it.sh
COPY --from=builder /app/meow-app /app/meow-app

# Make scripts executable
RUN chmod +x /app/wait-for-it.sh /app/meow-app

CMD ["./meow-app"]
