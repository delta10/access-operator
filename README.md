# Access operator

This project uses [Kubebuilder](https://kubebuilder.io/) to implement a Kubernetes operator that manages access to databases for different applications.


## Setup
First make sure you have [Kubebuilder](https://book.kubebuilder.io/quick-start.html#installation) installed on your machine. Follow the instructions on the Kubebuilder website to set it up.

If you're running Windows use WSL.

## Running the operator locally
First install the operator with the following command:
```bash
make install
```
Then run it:
```bash
make run
```

Ofcourse make sure you have a Database running that you can connect to, for example a local Postgres instance.   
You can use the following command to run a Postgres instance in Docker:
```bash
docker compose up -d
```

When using the docker compose you can use a simple sample config with:
```bash
kubectl apply -f config/samples/access_v1_postgresaccess.yaml
```

## Running tests
Ofcourse you can also use [act](https://github.com/nektos/act) to run the whole pipeline locally in Docker but preformance isn't great.

### Unit tests
```bash
make test
```
### e2e tests
Make sure the following are running and/or installed:
- kind
- kubectl
- Docker

for e2e tests, you can use:

```bash
make test-e2e
```

The database will be created withing the cluster using a custom resource, so you don't need to have a local database running for the e2e tests.

### Linter
Install [golangci-lint](https://golangci-lint.run/docs/welcome/install/local/) first, then you can run the linter with:

```bash
golangci-lint run
```
formatting issues can be fixed with:

```bash
golangci-lint fmt
```
