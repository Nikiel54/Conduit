# Build step
FROM golang:1.22-alpine AS builder

WORKDIR /app

# TODO: add go.sum later on when prometheus/client_golang is added.
COPY go.mod ./

RUN go mod download

COPY . .

# Compile the broker.
RUN CGO_ENABLED=0 go build \
    -ldflags="-s -w" \
    -o conduit \
    ./cmd/broker



# Runtime step

# The final image contains only:
#   - Alpine Linux (the OS, ~8 MB)
#   - The compiled conduit binary (~10 MB)
FROM alpine:3.20

# Creates a non-root user to run the broker.
RUN addgroup -S conduit && adduser -S -G conduit conduit

WORKDIR /app

COPY --from=builder /app/conduit .

# for WAL later
RUN mkdir -p /app/data && chown conduit:conduit /app/data

USER conduit 

# EXPOSE is documentation, not a firewall rule.
EXPOSE 8080

# VOLUME declares /app/data as a mount point for WAL files.
# Without this, WAL data lives inside the container layer and is lost when
# the container is removed.
VOLUME ["/app/data"]

# ENTRYPOINT makes conduit the process (PID 1), not a shell wrapping it.
# This means Docker's stop signal (SIGTERM) is delivered directly to the
# broker's signal handler which we need for shutdown.
ENTRYPOINT ["./conduit"]

# Default flag values.
CMD ["-listen", ":8080"]
