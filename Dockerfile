FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN go build -o /broker ./cmd/broker
RUN go build -o /manager ./cmd/manager
RUN go build -o /publisher ./cmd/publisher
RUN go build -o /subscriber ./cmd/subscriber
FROM alpine:3.19
RUN apk add --no-cache ca-certificates bash
WORKDIR /app
COPY --from=builder /broker /manager /publisher /subscriber /app/
VOLUME ["/app/data"]
EXPOSE 8080 8081
CMD ["/app/manager"]
 