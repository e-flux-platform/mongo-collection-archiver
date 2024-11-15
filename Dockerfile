FROM golang:1.23-bookworm AS build
WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY ./ ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /mongo-collection-archiver .

FROM gcr.io/distroless/base-debian12
WORKDIR /
COPY --from=build mongo-collection-archiver /mongo-collection-archiver
ENTRYPOINT [ "/mongo-collection-archiver" ]
