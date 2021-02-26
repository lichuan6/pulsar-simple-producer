all: producer consumer

producer:
	go build -o producer cmd/producer/main.go

consumer:
	go build -o consumer cmd/consumer/main.go
