# Access operator

Manages users and permissions for Postgres and RabbitMQ.

CloudNativePG is supported and validated.

## Install from a release
If you just want to install the operator on a pre-existing deployment skip to the [installing the operator](#installing-the-operator) section.

### Creating test resources first
These deployments should not be used in production, they are only meant for and validating the operator for your usage.

First create a namespace for the operator and the test resources:
```bash
kubectl create namespace access-operator-demo
```

#### Postgres
First make the following yaml file and apply it to your cluster:
```yaml
apiVersion: v1
kind: Service
metadata:
    name: postgres
    namespace: access-operator-demo
spec:
    selector:
        app: postgres
    ports:
        - name: postgres
          port: 5432
          targetPort: 5432
---
apiVersion: apps/v1
kind: Deployment
metadata:
    name: postgres
    namespace: %s
spec:
    replicas: 1
    selector:
        matchLabels:
            app: postgres
    template:
        metadata:
            labels:
                app: postgres
        spec:
            containers:
                - name: postgres
                  image: postgres:18.3-alpine
                  imagePullPolicy: IfNotPresent
                  env:
                      - name: POSTGRES_USER
                        value: postgres
                      - name: POSTGRES_PASSWORD
                        value: postgres
                      - name: POSTGRES_DB
                        value: postgres
                  ports:
                      - containerPort: 5432
```

#### RabbitMQ
First make the following yaml file and apply it to your cluster:
```yaml
apiVersion: v1
kind: Service
metadata:
    name: rabbitmq
    namespace: access-operator-demo
spec:
    selector:
        app: rabbitmq
    ports:
        - name: amqp
          port: 5672
          targetPort: 5672
        - name: management
          port: 15672
          targetPort: 15672
---
apiVersion: apps/v1
kind: Deployment
metadata:
    name: rabbitmq
    namespace: access-operator-demo
spec:
    replicas: 1
    selector:
        matchLabels:
            app: rabbitmq
    template:
        metadata:
            labels:
                app: rabbitmq
        spec:
            containers:
                - name: rabbitmq
                  image: rabbitmq:4.2.4-management-alpine
                  imagePullPolicy: IfNotPresent
                  env:
                      - name: RABBITMQ_DEFAULT_USER
                        value: app-user
                      - name: RABBITMQ_DEFAULT_PASS
                        value: app-user-password
                  ports:
                      - containerPort: 5672
                      - containerPort: 15672
```

## Installing the operator
Replace `<tag>` with the latest version, which can be found on the [releases page](https://github.com/delta10/access-operator/releases)
```bash
kubectl apply -f https://github.com/delta10/access-operator/releases/download/<tag>/install.yaml
```
For some sample custom resources, see the `config/samples/` directory.




# Connections within Custom Resources
In sample CR's you'll notice 3 ways of connecting to the service:

1. `Secret connection (reccomended)`: This is the cleanest method; you create an opaque Secret that contains the connection details, and then reference that Secret in the CR.  
   Use the same keys as direct connection or CloudNativePG.

2. `Direct connection with existing secret`: Same as direct connection, but but you refence the credentials in a secret instead.   
      This is more secure than the first option since the credentials are not stored in plain text within the CR.

3. `Direct connection`: Here you'll specify the connection details directly in the CR, such as host, port, username, and password. This is the simplest way to connect, but it can be less secure since the credentials are stored in plain text within the CR.




# Controller configuration
All of these settings are optional, and have safe defaults. If you don't need to change any of these settings, you can skip this section.

## Cross-namespace existing secrets
By default, the `existingSecret` value is resolved in the same namespace as the `PostgresAccess` resource.

To allow cross-namespace references, create exactly one `Controller` resource as found in `config/samples/access_v1_controller.yaml` in the same namespace as the operator.

If there are zero `Controller` resources, the safe default is `false`.
Only 1 controller resource is allowed, if there are more than 1, the operator will log an error and ignore all but the first one it finds.

Excluded usernames are skipped during normal reconciliation and orphan cleanup, so the operator will not create, update, or delete those roles.

When using the docker compose you can use a simple sample config with:
```bash
kubectl apply -f config/samples/access_v1_postgresaccess.yaml
```

## Ignoring users
To exclude certain usernames from being managed by the operator, you can specify them in the `Controller` resource as well. 
This is useful for excluding default users like `postgres` or `admin` that are Òcreated by the database itself.

Add the service's key within the settings as shown here in the CR to exclude the `postgres` and `admin` users:
```yaml
spec:
    settings:
    existingSecretNamespace: false
    postgres:
       excludedUsers:
         - postgres
    rabbitmq:
       excludedUsers:
         - admin
```