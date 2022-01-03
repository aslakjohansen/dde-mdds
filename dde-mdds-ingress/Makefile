TARGETS = \
	ingress \


all: ${TARGETS}

clean:
	rm ${TARGETS}


ingress: ingress.go
	go build ingress.go

docker: ingress Dockerfile
	docker build . -t dde-mdds-ingress

