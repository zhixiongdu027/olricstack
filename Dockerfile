FROM golang:1.26 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/olric-node ./cmd/olric-node
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/watchdog ./cmd/watchdog
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/operator ./cmd/operator

FROM scratch AS olric-node
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/olric-node /olric-node
ENTRYPOINT ["/olric-node"]

FROM scratch AS watchdog
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/watchdog /watchdog
ENTRYPOINT ["/watchdog"]

FROM scratch AS operator
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/operator /operator
ENTRYPOINT ["/operator"]
