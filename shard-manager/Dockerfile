FROM golang:1.21.7-bookworm as builder

WORKDIR /src/karma8-storage

COPY . .

RUN cd ./shard-manager && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o shard-manager

#Second, after fixing the paths inaccuracies in the Dockerfile, you run into a more subtle problem, 
#when trying to run the go binary: not found, because the the golang builder image is using 
#Debian GLIBC 2.31-13+deb11u5 2.31 while the runner image is using musl libc (x86_64) Version 1.2.2.
FROM alpine:3.18.2

WORKDIR /app

COPY --from=builder /src/karma8-storage/shard-manager/shard-manager .

CMD ["/app/shard-manager"]