//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/delta10/access-operator/internal/controller"
	"github.com/delta10/access-operator/test/utils"
)

type sharedBackend struct {
	namespace string
	conn      controller.ConnectionDetails
}

type specEnv struct {
	namespace        string
	backendNamespace string
	conn             controller.ConnectionDetails
	suffix           string
}

type postgresSpecEnv struct{ specEnv }

type rabbitMQSpecEnv struct{ specEnv }

var (
	postgresBackendOnce sync.Once
	postgresBackend     sharedBackend

	rabbitMQBackendOnce sync.Once
	rabbitMQBackend     sharedBackend
)

func workerID() string {
	return fmt.Sprintf("p%d", GinkgoParallelProcess())
}

func uniqueSuffix() string {
	return fmt.Sprintf("%s-%d", workerID(), time.Now().UnixNano()%1_000_000)
}

func createNamespace(name string) {
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %s
`, name)

	Expect(utils.ApplyManifest(manifest)).To(Succeed(), "Failed to create namespace %s", name)
}

func createTestNamespace(prefix string) string {
	name := fmt.Sprintf("%s-%s", prefix, uniqueSuffix())
	createNamespace(name)
	return name
}

func deleteNamespace(name string) {
	cmd := exec.Command("kubectl", "delete", "ns", name, "--ignore-not-found", "--wait=false")
	_, _ = utils.Run(cmd)
}

func clearAllControllers() {
	cmd := exec.Command("kubectl", "delete", "controller", "--all", "-A", "--ignore-not-found", "--wait=false")
	_, _ = utils.Run(cmd)
}

func ensureWorkerBackend(
	once *sync.Once,
	backend *sharedBackend,
	namespacePrefix string,
	connectionForNamespace func(string) controller.ConnectionDetails,
	setup func(string, controller.ConnectionDetails),
) (string, controller.ConnectionDetails) {
	once.Do(func() {
		backendNamespace := fmt.Sprintf("%s-%s", namespacePrefix, workerID())
		deleteNamespace(backendNamespace)
		createNamespace(backendNamespace)

		conn := connectionForNamespace(backendNamespace)
		setup(backendNamespace, conn)

		*backend = sharedBackend{
			namespace: backendNamespace,
			conn:      conn,
		}
	})

	return backend.namespace, backend.conn
}

func ensurePostgresWorkerBackend() (string, controller.ConnectionDetails) {
	return ensureWorkerBackend(
		&postgresBackendOnce,
		&postgresBackend,
		"postgres-backend",
		utils.DatabaseConnectionDetailsForNamespace,
		func(backendNamespace string, conn controller.ConnectionDetails) {
			Expect(utils.DeployPostgresInstance(backendNamespace, conn)).To(Succeed(),
				"Failed to deploy shared PostgreSQL backend")

			cmd := exec.Command(
				"kubectl",
				"wait",
				"--for=condition=Available",
				"deployment/postgres",
				"-n",
				backendNamespace,
				"--timeout=2m",
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "PostgreSQL backend deployment should become available")

			Eventually(func(g Gomega) {
				output, queryErr := utils.RunPostgresQuery(backendNamespace, conn, "SELECT 1;")
				g.Expect(queryErr).NotTo(HaveOccurred(), "Shared PostgreSQL backend should accept connections")
				g.Expect(output).To(Equal("1"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			_, err = utils.RunPostgresQuery(
				backendNamespace,
				conn,
				"CREATE TABLE IF NOT EXISTS public.access_operator_test(id SERIAL PRIMARY KEY, value TEXT);",
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to prepare shared PostgreSQL backend table")
		},
	)
}

func ensureRabbitMQWorkerBackend() (string, controller.ConnectionDetails) {
	return ensureWorkerBackend(
		&rabbitMQBackendOnce,
		&rabbitMQBackend,
		"rabbitmq-backend",
		utils.RabbitMQConnectionDetailsForNamespace,
		func(backendNamespace string, conn controller.ConnectionDetails) {
			Expect(utils.DeployRabbitMQInstance(backendNamespace, conn)).To(Succeed(),
				"Failed to deploy shared RabbitMQ backend")
			utils.WaitForRabbitMQReady(backendNamespace)
		},
	)
}

func cleanupWorkerBackends() {
	if postgresBackend.namespace != "" {
		deleteNamespace(postgresBackend.namespace)
	}
	if rabbitMQBackend.namespace != "" {
		deleteNamespace(rabbitMQBackend.namespace)
	}
}

func newSpecEnv(prefix string, ensureBackend func() (string, controller.ConnectionDetails)) specEnv {
	backendNamespace, conn := ensureBackend()
	return specEnv{
		namespace:        createTestNamespace(prefix),
		backendNamespace: backendNamespace,
		conn:             conn,
		suffix:           uniqueSuffix(),
	}
}

func (e specEnv) cleanup() {
	deleteNamespace(e.namespace)
}

func (e specEnv) name(base string) string {
	return fmt.Sprintf("%s-%s", base, e.suffix)
}

func newPostgresSpecEnv() postgresSpecEnv {
	return postgresSpecEnv{specEnv: newSpecEnv("postgres-access", ensurePostgresWorkerBackend)}
}

func newRabbitMQSpecEnv() rabbitMQSpecEnv {
	return rabbitMQSpecEnv{specEnv: newSpecEnv("rabbitmq-access", ensureRabbitMQWorkerBackend)}
}

func (e rabbitMQSpecEnv) vhost(base string) string {
	return "/" + e.name(base)
}
