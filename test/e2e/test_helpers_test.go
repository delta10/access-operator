//go:build e2e
// +build e2e

package e2e

import (
    "fmt"
    "os/exec"
    "strings"
    "time"

    utils2 "github.com/delta10/access-operator/test/e2e/utils"
    . "github.com/onsi/gomega"
)

type namespacedName struct {
	name      string
	namespace string
}

type readyConditionExpectation struct {
	status          string
	reason          string
	messageContains string
}

func waitForReadyCondition(
	resourceType string,
	resource namespacedName,
	expectation readyConditionExpectation,
) {
	Eventually(func(g Gomega) {
		if expectation.status != "" {
			status, err := getReadyConditionField(resourceType, resource, "status")
			g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve Ready condition status")
			g.Expect(status).To(Equal(expectation.status))
		}

		if expectation.reason != "" {
			reason, err := getReadyConditionField(resourceType, resource, "reason")
			g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve Ready condition reason")
			g.Expect(reason).To(Equal(expectation.reason))
		}

		if expectation.messageContains != "" {
			message, err := getReadyConditionField(resourceType, resource, "message")
			g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve Ready condition message")
			g.Expect(message).To(ContainSubstring(expectation.messageContains))
		}
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

func getReadyConditionField(resourceType string, resource namespacedName, field string) (string, error) {
	cmd := exec.Command(
		"kubectl",
		"get",
		resourceType,
		resource.name,
		"-n",
		resource.namespace,
		"-o",
		fmt.Sprintf("jsonpath={.status.conditions[?(@.type=='Ready')].%s}", field),
	)
	output, err := utils2.Run(cmd)
	return strings.TrimSpace(output), err
}

func waitForControllerLogsContain(substrings ...string) {
	Eventually(func(g Gomega) {
		controllerPodName = ensureControllerPodName()
		cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace, "--since=10m")
		output, err := utils2.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to read controller logs")
		for _, substring := range substrings {
			g.Expect(output).To(ContainSubstring(substring))
		}
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

func createControllerResource(name, namespace, settingsYAML string) error {
	settingsYAML = strings.TrimSpace(settingsYAML)
	if settingsYAML == "" {
		return fmt.Errorf("controller settings YAML cannot be empty")
	}

	manifest := fmt.Sprintf(`apiVersion: access.k8s.delta10.nl/v1
kind: Controller
metadata:
  name: %s
  namespace: %s
spec:
  settings:
%s
`, name, namespace, indentYAMLBlock(settingsYAML, "    "))

	return utils2.ApplyManifest(manifest)
}

func deleteControllerResource(name, namespace string) {
	cmd := exec.Command("kubectl", "delete", "controller", name, "-n", namespace, "--ignore-not-found", "--wait=false")
	_, _ = utils2.Run(cmd)

	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "controller", name, "-n", namespace, "-o", "name", "--ignore-not-found")
		output, err := utils2.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(output)).To(BeEmpty())
	}, 30*time.Second, time.Second).Should(Succeed())
}

func waitForResourceWarningEvent(resource namespacedName, kind, reason string) {
	Eventually(func(g Gomega) {
		cmd := exec.Command(
			"kubectl",
			"get",
			"events",
			"-n",
			resource.namespace,
			"--field-selector",
			fmt.Sprintf("involvedObject.kind=%s,involvedObject.name=%s,reason=%s", kind, resource.name, reason),
			"--no-headers",
		)
		output, err := utils2.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(output)).NotTo(BeEmpty())
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

func waitForControllerResourcesReadyCondition(resources []namespacedName, expectation readyConditionExpectation) {
	Eventually(func(g Gomega) {
		for _, resource := range resources {
			if expectation.status != "" {
				status, err := getReadyConditionField("controller", resource, "status")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal(expectation.status))
			}

			if expectation.reason != "" {
				reason, err := getReadyConditionField("controller", resource, "reason")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(reason).To(Equal(expectation.reason))
			}
		}
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

func waitForNoControllers() {
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "controller", "-A", "-o", "name", "--ignore-not-found")
		output, err := utils2.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(output)).To(BeEmpty())
	}, 30*time.Second, time.Second).Should(Succeed())
}

func indentYAMLBlock(block, indent string) string {
	lines := strings.Split(strings.TrimSpace(block), "\n")
	for i, line := range lines {
		lines[i] = indent + line
	}
	return strings.Join(lines, "\n")
}
