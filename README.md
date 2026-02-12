# Access operator

This project uses [Kubebuilder](https://kubebuilder.io/) to implement a Kubernetes operator that manages access to databases for different applications.


## Setup
First make sure you have [Kubebuilder](https://book.kubebuilder.io/quick-start.html#installation) installed on your machine. Follow the instructions on the Kubebuilder website to set it up.

If you're running windows use WSL.

## Running tests
Ofcourse you can also use [act](https://github.com/nektos/act) to run the whole pipeline locally in Docker but preformance isn't great.

### Unit tests
```bash
make test
```
### e2e tests
for e2e tests, you can use:

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
