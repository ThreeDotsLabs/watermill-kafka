up:
	docker-compose up

test: wait
	go test -parallel 20 ./...

test_v: wait
	go test -parallel 20 -v ./...

test_short: wait
	go test -parallel 20 ./... -short

test_race: wait
	go test ./... -short -race

test_stress: wait
	go test -v -tags=stress -parallel 15 -timeout=45m ./...

test_reconnect: wait
	go test -tags=reconnect ./...

fmt:
	go fmt ./...
	goimports -l -w .

build:
	go build ./...

wait:
	go run .github/workflows/wait-for.go localhost:9091 localhost:9092 localhost:9093 localhost:9094 localhost:9095

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt

