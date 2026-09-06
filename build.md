### Docker
```
docker build -t pg2sqs:0.8 --file Dockerfile_pg2sqs .
docker save pg2sqs:0.8 -o pg2sqs.tar
```

### Push pg2redis to Docker Hub

Log in to Docker Hub:

```bash
docker login
```

Build the local image. The Makefile injects `PG2REDIS_VERSION` into
`replicator.Version` and tags the image as
`PG2REDIS_IMAGE:PG2REDIS_VERSION`.

```bash
make docker-pg2redis PG2REDIS_VERSION=0.4.2 PG2REDIS_IMAGE=alikpgwalk/pg2redis
```

Push the versioned image:

```bash
docker push alikpgwalk/pg2redis:0.4.2
```

To push to a private Docker Hub repository, set `PG2REDIS_IMAGE` to that
repository name:

```bash
make docker-pg2redis PG2REDIS_VERSION=0.4.2 PG2REDIS_IMAGE=<dockerhub-user>/<private-repo>
docker push <dockerhub-user>/<private-repo>:0.4.2
```

### SSH

- ```
    sudo apt update
    sudo apt install openssh-server
    sudo systemctl enable ssh
    sudo systemctl start ssh
    ```
- detect IP address -> ```hostname -I```
- make sure user name is correct - use <b>whoami</b>
- ```ssh <username>@<ip>```
- copy tar by scp ```scp myapp.tar user@linux-laptop:/tmp```
- sudo ctr images import <path to tar>

### Linux
```
psql -h cluster-example-rw -U postgres -d app
kubectl get secret cluster-example-superuser -o jsonpath="{.data.password}" | base64 -d
kubectl run psql-client --rm -i -t --image=postgres -- bash
```
