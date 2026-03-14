FROM golang:1.25-alpine

WORKDIR /app

COPY go.mod go.sum /app
RUN go mod download

COPY ./Consumers /app/Consumers
COPY ./shared /app/shared 


## FOR PRODUCTION DO MULTI STAGE
# RUN go build -o /app/bin/a ./consumerA
# RUN go build -o /app/bin/b ./consumerB
# RUN go build -o /app/bin/c ./consumerC
# RUN go build -o /app/bin/d ./consumerD

# CMD ["/app/bin/a"]
