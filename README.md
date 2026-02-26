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

## Install from a release
Every tagged release publishes an `install.yaml` asset that references the prebuilt image on GHCR.

```bash
kubectl apply -f https://github.com/<org>/<repo>/releases/download/<tag>/install.yaml
```

If you wish to install the operator via its yaml you can find it in `config/crd/bases/access.k8s.delta10.nl_postgresaccesses.yaml`

Then run it:
```bash
make run
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
for e2e tests, you can use:
Make sure you have [kind](https://kind.sigs.k8s.io/) installed and running, then you can run the e2e tests with:
```bash
make test-e2e
```

if you don't have a database running locally you can use docker compose to start a postgres database:

```bash
docker compose up -d
```

### Linter
Install [golangci-lint](https://golangci-lint.run/docs/welcome/install/local/) first, then you can run the linter with:

```bash
golangci-lint run
```
formatting issues can be fixed with:

```bash
golangci-lint fmt
```
