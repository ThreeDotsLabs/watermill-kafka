up:
	docker-compose up -d

test:
	go test ./...

test_v:
	go test -v ./...

test_short:
	go test ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -v -tags=stress -timeout=45m ./...

test_reconnect:
	go test -tags=reconnect ./...

fmt:
	go fmt ./...
	goimports -l -w .

build:
	go build ./...

wait:
	go run github.com/ThreeDotsLabs/wait-for@latest localhost:9091 localhost:9092 localhost:9093 localhost:9094 localhost:9095

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt

