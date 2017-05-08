# Build container
FROM golang:1.8 as builder

WORKDIR /go/src/github.com/msales/kage/
COPY ./ .

RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-s' -o kage ./cmd/kage

# Run container
FROM scratch

COPY --from=builder /go/src/github.com/msales/kage/kage .

ENV KAGE_ADDR ":80"

EXPOSE 80
CMD ["./kage"]