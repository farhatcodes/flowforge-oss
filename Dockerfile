FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /flowforge ./cmd/flowforge
FROM scratch
COPY --from=build /flowforge /flowforge
USER 65532:65532
ENTRYPOINT ["/flowforge"]
