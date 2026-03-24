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

package controller

import (
	"context"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	accessv1 "github.com/delta10/access-operator/api/v1"
	// +kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var (
	ctx       = context.Context(nil)
	cancel    = context.CancelFunc(nil)
	testEnv   = (*envtest.Environment)(nil)
	cfg       = (*rest.Config)(nil)
	k8sClient client.Client
)

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	state, err := BootstrapEnvTestSuite(
		GinkgoWriter,
		filepath.Join("..", "..", "config", "crd", "bases"),
		filepath.Join("..", "..", "bin", "k8s"),
		func() error {
			if err := accessv1.AddToScheme(scheme.Scheme); err != nil {
				return err
			}

			// +kubebuilder:scaffold:scheme

			return nil
		},
	)
	Expect(err).NotTo(HaveOccurred())

	ctx = state.Ctx
	cancel = state.Cancel
	testEnv = state.Env
	cfg = state.Config
	k8sClient = state.Client
})

var _ = AfterSuite(func() {
	TeardownEnvTestSuite(EnvTestSuiteState{
		Ctx:    ctx,
		Cancel: cancel,
		Env:    testEnv,
		Config: cfg,
		Client: k8sClient,
	})
})
