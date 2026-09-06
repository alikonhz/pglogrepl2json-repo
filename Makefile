.PHONY: test
test:
	go test ./replicator/*

.PHONY: run
run:
	go run ./example/cmd/main.go

.PHONY: run-redis
run-redis:
	go run ./pg2redis/cmd/main.go


.PHONY: build-amd64
build-amd64:
	set GOOS=linux&& set GOARCH=amd64&& go build -ldflags="-s -w" -o ./pg2redis/output/al2023-x64/pg2redis.out ./pg2redis/cmd/main.go

.PHONY: build-amd64-sqs
build-amd64-sqs:
	set GOOS=linux&& set GOARCH=amd64&& go build -ldflags "-s -w -X 'github.com/alikonhz/pglogrepl2json/replicator.Version=0.9.5'" -o ./pg2sqs/output/linux-x64/pg2sqs.out ./pg2sqs/cmd/main.go

.PHONY: build-amd64-redis
build-amd64-redis:
	set GOOS=linux&& set GOARCH=amd64&& go build -ldflags="-s -w" -o ./pg2redis/output/linux-x64/pg2redis.out ./pg2redis/cmd/main.go

# run build-amd64 before running packer
.PHONY: redis-packer-al2023-x64
redis-packer-al2023-x64:
	packer build ./redis-aws-al2023-x64.pkr

# run build-amd64 before running packer
.PHONY: sqs-packer-al2023-x64
sqs-packer-al2023-x64:
	packer build ./sqs-aws-al2023-x64.pkr.hcl

.PHONY: sqs-packer-ubuntu2404-x64
sqs-packer-ubuntu2404-x64:
	packer build ./sqs-aws-ubuntu2404-x64.pkr.hcl

.PHONY: docker-pg2sqs
docker-pg2sqs:
	docker build --build-arg BUILD_VERSION=0.9.5 -t alikpgwalk/pg2sqs:0.9.5 -t alikpgwalk/pg2sqs:latest --file Dockerfile_pg2sqs .

PG2REDIS_VERSION ?= 0.4.3
PG2REDIS_IMAGE ?= alikpgwalk/pg2redis

.PHONY: docker-pg2redis
docker-pg2redis:
	docker build --build-arg BUILD_VERSION=$(PG2REDIS_VERSION) -t $(PG2REDIS_IMAGE):$(PG2REDIS_VERSION) -t $(PG2REDIS_IMAGE):latest --file Dockerfile_pg2redis .
