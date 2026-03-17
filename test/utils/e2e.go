//go:build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" // nolint:revive,staticcheck
	. "github.com/onsi/gomega"    // nolint:revive,staticcheck
)

// ServiceAccountToken returns a token for the given service account in a namespace.
func ServiceAccountToken(namespace, serviceAccountName string) (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join(os.TempDir(), secretName)
	if err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), 0o644); err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())

		var token tokenRequest
		err = json.Unmarshal([]byte(output), &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, nil
}

// GetMetricsOutput returns logs from the metrics curl pod.
func GetMetricsOutput(namespace, podName string) (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", podName, "-n", namespace)
	return Run(cmd)
}

// ApplyManifest applies the provided manifest through kubectl.
func ApplyManifest(manifest string) error {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	// Raw multiline strings in Go can accidentally include leading/trailing
	// indentation from the closing backtick line, which breaks YAML parsing.
	cmd.Stdin = strings.NewReader(strings.TrimSpace(manifest) + "\n")
	_, err := Run(cmd)
	return err
}

// WaitForSecretField waits until a secret data field is present and returns its value.
func WaitForSecretField(namespace, secretName, field string) string {
	var output string
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "secret", secretName, "-n", namespace, "-o", fmt.Sprintf("jsonpath={.data.%s}", field))
		var err error
		output, err = Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve secret field")
		g.Expect(strings.TrimSpace(output)).NotTo(BeEmpty(), "Secret field should not be empty")
	}, 2*time.Minute, 5*time.Second).Should(Succeed())

	return strings.TrimSpace(output)
}

// WaitForDecodedSecretField waits for a secret field and returns decoded base64 data.
func WaitForDecodedSecretField(namespace, secretName, field string) string {
	encoded := WaitForSecretField(namespace, secretName, field)
	decoded, err := b64.StdEncoding.DecodeString(encoded)
	Expect(err).NotTo(HaveOccurred(), "Secret value should be valid base64")
	return string(decoded)
}

// WaitForSecretDeleted waits until a secret is no longer found.
func WaitForSecretDeleted(namespace, secretName string) {
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "secret", secretName, "-n", namespace, "-o", "name", "--ignore-not-found")
		output, err := Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to check if secret exists")
		g.Expect(strings.TrimSpace(output)).To(BeEmpty(), "Secret should have been deleted")
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// WaitForResourceDeleted waits until a namespaced resource is no longer found.
func WaitForResourceDeleted(resourceType, name, namespace string) {
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", resourceType, name, "-n", namespace, "-o", "name", "--ignore-not-found")
		output, err := Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to check if resource exists")
		g.Expect(strings.TrimSpace(output)).To(BeEmpty(), "Resource should have been deleted")
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// TriggerReconciliation forces a reconcile by updating an annotation.
func TriggerReconciliation(resourceType, resourceName, namespace string) error {
	timestamp := time.Now().Format(time.RFC3339Nano)
	cmd := exec.Command("kubectl", "annotate", resourceType, resourceName,
		"-n", namespace,
		fmt.Sprintf("reconcile-trigger=%s", timestamp),
		"--overwrite")
	_, err := Run(cmd)
	return err
}

func formatStringListYAML(items []string, indent string) string {
	lines := make([]string, 0, len(items))
	for _, item := range items {
		lines = append(lines, fmt.Sprintf("%s- %s", indent, item))
	}
	return strings.Join(lines, "\n")
}

type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
