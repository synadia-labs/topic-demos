FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum main.go ./
COPY static ./static
RUN go build -mod=mod -o /demo .

FROM alpine:3.20
COPY --from=builder /demo /demo
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/demo"]
