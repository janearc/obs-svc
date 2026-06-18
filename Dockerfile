# Stage 1: build the static binary, generating protobuf bindings at build time.
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src

# Codegen toolchain (cached layer): buf + the Go plugin. Bindings are generated
# from the vendored proto and never committed.
RUN go install github.com/bufbuild/buf/cmd/buf@v1.71.0 \
 && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ENV PATH="/go/bin:${PATH}"

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN buf generate
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o obs-svc-agg ./cmd/obs-svc-agg

# Stage 2: scratch runtime.
FROM scratch
COPY --from=builder /src/obs-svc-agg /usr/local/bin/obs-svc-agg
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/obs-svc-agg"]
