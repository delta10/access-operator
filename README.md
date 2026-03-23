# Access operator

Manages users and permissions for Postgres and RabbitMQ.

CloudNativePG is supported.

## Overview
This Operator makes sure only users that are defined in the custom resources have access to the services, and that they have the correct permissions.  
Users outside the custom resources will be removed, and users (and vHosts) within the custom resources will be created or updated if they already exist.  
It's possible to exclude certain users from this flow, see the [Ignoring users](#ignoring-users) section for more details.

When a user is created a random password is generated and stored in a Secret (the secret's name is defined in the CR).  
If you wish to rotate a password delete the secret in Kubernetes and a new one will be generated and applied.


## Installing the operator
Replace `<tag>` with the latest version, which can be found on the [releases page](https://github.com/delta10/access-operator/releases)
```bash
kubectl apply -f https://github.com/delta10/access-operator/releases/download/<tag>/install.yaml
```
Every user needs to be defined in its own custom resource.  
For some sample custom resources, see the `config/samples/` directory.  
Make sure to read the [Creating custom resources](#creating-custom-resources) section as well as the descriptions within the CRD when making a CR.

## Creating test resources
To validate this operator within a test cluster you can follow these instructions to quickly make some test resources.  
These deployments should not be used in production.

First create a namespace for the operator and the test resources:
```bash
kubectl create namespace access-operator-demo
```

#### Postgres
Then make the following yaml file and apply it to your cluster:
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
    namespace: access-operator-demo
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
After creating the Postgres operator make the following yaml file and apply it to your cluster:
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

### Applying the sample CRs
After creating the test resources, you can apply the sample CRs to create users and permissions for those services. 
If you use the used the test resources above, you can apply the sample CRs using the following command:
```bash
kubectl apply -f config/samples/access_v1_postgresaccess.yaml
```

# Creating custom resources

## Connections within Custom Resources
In sample CR's you'll notice 3 ways of connecting to the service:

1. `Secret connection (recommended)`: This is the cleanest method that avoids duplication of connection details.  
    You create an opaque Secret that contains the connection details, and then reference that Secret in the CR.
    Use the same keys as connection details or CloudNativePG.

2. `Connection details with existing secret`: Same as 3, but you refence the username and password in a secret instead.   
    This is the production ready version of 3 since the credentials are not stored in plain text within the CR.

3. `Connection details`: Here you'll specify the connection details directly in the CR, such as host, port, username, and password.   
    This is the simplest way to connect, but it's discouraged for production use since the credentials are stored in plain text within the CR.


# Controller configuration
All of these settings are optional, and have safe defaults. If you don't need to change any of these settings, you can skip this section.

Only 1 controller resource is allowed, if there are more than 1, the operator will log an error and ignore all but the first one it finds.


## Cross-namespace existing secrets
By default, the `existingSecret` value is resolved in the same namespace as the `PostgresAccess` resource.

To allow cross-namespace references, create exactly one `Controller` resource as found in `config/samples/access_v1_controller.yaml` in the same namespace as the operator.  
Excluded usernames are skipped during normal reconciliation and orphan cleanup, so the operator will not create, update, or delete those roles.

## Ignoring users
By default, no users are ignored **except** for Postgres users that can't login (rolcanlogin == false) or are superusers (rolsuper == true).
To exclude certain usernames from being managed by the operator, you can specify them in the `Controller` resource as well.   
This is useful for excluding default users like `postgres` or `admin` that are created by the service itself for example.

Add the service's key (postgres, rabbitmq) within the settings as shown here in the CR to exclude the `postgres` and `admin` users:
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