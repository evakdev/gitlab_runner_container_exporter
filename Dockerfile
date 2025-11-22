FROM golang:alpine as builder
# RUN echo "http://mirror.clarkson.edu/alpine/v3.22/main" > /etc/apk/repositories && \
#     echo "http://mirror.clarkson.edu/alpine/v3.22/community" >> /etc/apk/repositories && \
#     apk --no-cache add git ca-certificates

RUN apk update && apk --no-cache add git && apk --no-cache add ca-certificates
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o /go/bin/gitlab_runner_container_exporter .

FROM alpine:3
COPY --from=builder /go/bin/gitlab_runner_container_exporter /go/bin/gitlab_runner_container_exporter
EXPOSE 8080
ENTRYPOINT ["/go/bin/gitlab_runner_container_exporter"]
CMD ["-listen-address=:8080"]