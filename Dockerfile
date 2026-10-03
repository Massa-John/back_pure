FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go .
RUN CGO_ENABLED=0 go build -o /messenger .

FROM alpine:3.20
#RUN adduser -D app
#USER app
COPY --from=build /messenger /messenger
EXPOSE 8080
CMD ["/messenger"]
