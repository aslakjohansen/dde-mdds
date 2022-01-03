TARGETS = \
	process \


all: ${TARGETS}

clean:
	rm ${TARGETS}


process: process.go
	go build process.go

docker: process Dockerfile workers/*
	docker build . -t dde-mdds-process

