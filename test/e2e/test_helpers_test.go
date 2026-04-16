//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/gomega"

	operatorcontroller "github.com/delta10/access-operator/internal/controller"
	e2eutils "github.com/delta10/access-operator/test/e2e/utils"
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
	output, err := e2eutils.Run(cmd)
	return strings.TrimSpace(output), err
}

func waitForControllerLogsContain(substrings ...string) {
	Eventually(func(g Gomega) {
		controllerPodName = ensureControllerPodName()
		cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace, "--since=10m")
		output, err := e2eutils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to read controller logs")
		for _, substring := range substrings {
			g.Expect(output).To(ContainSubstring(substring))
		}
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

func createControllerSettingsConfigMap(namespace, settingsYAML string) error {
	settingsYAML = strings.TrimSpace(settingsYAML)
	if settingsYAML == "" {
		return fmt.Errorf("controller settings YAML cannot be empty")
	}

	manifest := fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: %s
  namespace: %s
data:
  %s: |
%s
`, operatorcontroller.ControllerSettingsConfigMapName, namespace, operatorcontroller.ControllerSettingsConfigMapKey, indentYAMLBlock(settingsYAML, "        "))

	return e2eutils.ApplyManifest(manifest)
}

func deleteControllerSettingsConfigMap(namespace string) {
	cmd := exec.Command(
		"kubectl",
		"delete",
		"configmap",
		operatorcontroller.ControllerSettingsConfigMapName,
		"-n",
		namespace,
		"--ignore-not-found",
		"--wait=false",
	)
	_, _ = e2eutils.Run(cmd)

	Eventually(func(g Gomega) {
		cmd := exec.Command(
			"kubectl",
			"get",
			"configmap",
			"-n",
			namespace,
			operatorcontroller.ControllerSettingsConfigMapName,
			"-o",
			"name",
			"--ignore-not-found",
		)
		output, err := e2eutils.Run(cmd)
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
		output, err := e2eutils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(output)).NotTo(BeEmpty())
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

func waitForNoControllerSettingsConfigMaps() {
	Eventually(func(g Gomega) {
		configMaps, err := listControllerSettingsConfigMaps()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(configMaps).To(BeEmpty())
	}, 30*time.Second, time.Second).Should(Succeed())
}

func indentYAMLBlock(block, indent string) string {
	lines := strings.Split(strings.TrimSpace(block), "\n")
	for i, line := range lines {
		lines[i] = indent + line
	}
	return strings.Join(lines, "\n")
}

func listControllerSettingsConfigMaps() ([]namespacedName, error) {
	cmd := exec.Command(
		"kubectl",
		"get",
		"configmap",
		"-A",
		"--field-selector",
		fmt.Sprintf("metadata.name=%s", operatorcontroller.ControllerSettingsConfigMapName),
		"-o",
		`jsonpath={range .items[*]}{.metadata.namespace}{"\t"}{.metadata.name}{"\n"}{end}`,
	)
	output, err := e2eutils.Run(cmd)
	if err != nil {
		return nil, err
	}

	trimmedOutput := strings.TrimSpace(output)
	if trimmedOutput == "" {
		return nil, nil
	}

	lines := strings.Split(trimmedOutput, "\n")
	configMaps := make([]namespacedName, 0, len(lines))
	for _, line := range lines {
		fields := strings.SplitN(strings.TrimSpace(line), "\t", 2)
		if len(fields) != 2 {
			return nil, fmt.Errorf("unexpected configmap listing output %q", line)
		}

		configMaps = append(configMaps, namespacedName{
			namespace: fields[0],
			name:      fields[1],
		})
	}

	return configMaps, nil
}
