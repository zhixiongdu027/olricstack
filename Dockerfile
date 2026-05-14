FROM golang:1.25 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/olric-node ./cmd/olric-node
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/watchdog ./cmd/watchdog
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/operator ./cmd/operator

FROM gcr.io/distroless/static-debian12 AS olric-node
COPY --from=build /out/olric-node /olric-node
ENTRYPOINT ["/olric-node"]

FROM gcr.io/distroless/static-debian12 AS watchdog
COPY --from=build /out/watchdog /watchdog
ENTRYPOINT ["/watchdog"]

FROM gcr.io/distroless/static-debian12 AS operator
COPY --from=build /out/operator /operator
ENTRYPOINT ["/operator"]
