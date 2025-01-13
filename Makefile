up:
	docker compose up -d --wait

test:
	go test ./...

test_v:
	go test -v ./...

test_short:
	go test ./... -short

test_race:
	go test ./... -short -race

test_stress:
	STRESS_TEST_COUNT=4 go test -v -tags=stress -timeout=45m ./...

test_reconnect:
	go test -tags=reconnect ./...

test_codecov: up
	go test -coverprofile=coverage.out -covermode=atomic ./...

fmt:
	go fmt ./...
	goimports -l -w .

build:
	go build ./...

wait:

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt

