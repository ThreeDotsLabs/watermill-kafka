up:
	docker-compose up -d

test:
	go test -parallel 20 ./...

test_v:
	go test -parallel 20 -v ./...

test_short:
	go test -parallel 20 ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -v -tags=stress -parallel 15 -timeout=45m ./...

test_reconnect:
	go test -tags=reconnect ./...

fmt:
	go fmt ./...
	goimports -l -w .

build:
	go build ./...

wait:
	time go run github.com/ThreeDotsLabs/watermill/dev/wait-for@github-actions || true
	time go run github.com/ThreeDotsLabs/watermill/dev/wait-for@github-actions localhost:9091 localhost:9092 localhost:9093 localhost:9094 localhost:9095

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt

