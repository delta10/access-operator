//go:build e2e
// +build e2e

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

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/delta10/access-operator/test/utils"
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage = "example.com/access-operator:v0.0.1"
	// shouldCleanupCertManager tracks whether CertManager was installed by this suite.
	shouldCleanupCertManager = false
)

// TestE2E runs the e2e test suite to validate the solution in an isolated environment.
// The default setup requires Kind and CertManager.
//
// To skip CertManager installation, set: CERT_MANAGER_INSTALL_SKIP=true
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting access-operator e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")

	// TODO(user): If you want to change the e2e test vendor from Kind,
	// ensure the image is built and available, then remove the following block.
	By("loading the manager image on Kind")
	err = utils.LoadImageToKindClusterWithName(managerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")

	setupCertManager()

	By("creating manager namespace")
	cmd = exec.Command("kubectl", "create", "ns", namespace)
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to create namespace")

	By("labeling the namespace to enforce the restricted security policy")
	cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
		"pod-security.kubernetes.io/enforce=restricted")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

	By("installing CRDs")
	cmd = exec.Command("make", "install")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to install CRDs")

	By("waiting for operator CRDs to become established")
	err = utils.WaitForCRDsEstablished(
		"controllers.access.k8s.delta10.nl",
		"postgresaccesses.access.k8s.delta10.nl",
		"rabbitmqaccesses.access.k8s.delta10.nl",
		"redisaccesses.access.k8s.delta10.nl",
	)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to wait for operator CRDs")

	By("deploying the controller-manager")
	cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	return nil
}, func(_ []byte) {
	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)
})

var _ = SynchronizedAfterSuite(func() {
	cleanupWorkerBackends()
}, func() {
	runCleanup := func(timeout time.Duration, name string, args ...string) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, name, args...)
		_, _ = utils.Run(cmd)
	}

	By("cleaning up the curl pod for metrics")
	runCleanup(30*time.Second,
		"kubectl", "delete", "pod", "curl-metrics", "-n", namespace,
		"--ignore-not-found=true", "--wait=false")

	By("cleaning up the metrics role binding")
	runCleanup(30*time.Second,
		"kubectl", "delete", "clusterrolebinding", metricsRoleBindingName,
		"--ignore-not-found=true")

	By("undeploying the controller-manager")
	runCleanup(90*time.Second, "make", "undeploy", "ignore-not-found=true")

	By("uninstalling CRDs")
	runCleanup(90*time.Second, "make", "uninstall", "ignore-not-found=true")

	By("removing manager namespace")
	runCleanup(30*time.Second,
		"kubectl", "delete", "ns", namespace,
		"--ignore-not-found=true", "--wait=false")

	teardownCertManager()
})

// setupCertManager installs CertManager if needed for webhook tests.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if already present.
func setupCertManager() {
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	By("checking if CertManager is already installed")
	if utils.IsCertManagerCRDsInstalled() {
		_, _ = fmt.Fprintf(GinkgoWriter, "CertManager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	By("installing CertManager")
	Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install CertManager")
}

// teardownCertManager uninstalls CertManager if it was installed by setupCertManager.
// This ensures we only remove what we installed.
func teardownCertManager() {
	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager cleanup (not installed by this suite)\n")
		return
	}

	By("uninstalling CertManager")
	utils.UninstallCertManager()
}
