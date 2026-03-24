# Access operator developer/contributor guide

This project uses [Kubebuilder](https://kubebuilder.io/) to implement a Kubernetes operator that manages access to Postgres, Redis and RabbitMQ.

## Setup
First make sure you have [Kubebuilder](https://book.kubebuilder.io/quick-start.html#installation) installed on your machine. Follow the instructions on the Kubebuilder website to set it up.

If you're running Windows use WSL, this project doesn't work in Windows natively.

## Running the operator locally
First install the operator with the following command:
```bash
make install
```
If you wish to install the operator via its yaml you can find all CRDs it in `config/crd/bases/`

Then run a local instance of the operator with:
```bash
make run
```

## Adding a controller
For the repository-specific workflow for new controllers, see [`internal/controller/README.md`](internal/controller/README.md).

## Running tests
Of course you can also use [act](https://github.com/nektos/act) to run the whole pipeline locally in Docker but performance isn't great.

### Unit tests
```bash
make test
```
### e2e tests
For e2e tests:
Make sure you have [kind](https://kind.sigs.k8s.io/) installed and running, then you can run the e2e tests with:
```bash
make test-e2e
```
Though beware it'll consume quite a bit of memory, within WSL2 it consumed 9gb during the test run, MacOS did roughly 7gb.  
A machine with 24gb of memory is highly recommended for running the e2e tests while developing other things.

If you only want to run a specific context use the following command:
```bash
make test-e2e RUN_ONLY=postgres
```
This will only run the tests in the `postgres` context.

### Linting
This project uses [golangci-lint](https://golangci-lint.run/) for linting and formatting the codebase, you can run it with:
```bash
make lint-fix
```
