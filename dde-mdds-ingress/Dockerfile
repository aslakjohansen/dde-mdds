FROM ubuntu:20.04

COPY ingress.go /app/
COPY Makefile /app/

WORKDIR /app

# setup timezone to avoid the need for user input
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Copenhagen
#RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# install golang, git and make
RUN apt-get update
RUN apt-get -y install golang git make

# setup golang
ENV GO_PATH /app/go
RUN mkdir go

# build
RUN go get github.com/confluentinc/confluent-kafka-go/kafka
RUN go get github.com/lib/pq
RUN make

CMD [ "./ingress" ]

