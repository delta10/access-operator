package testsupport

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

type EnvTestSuiteState struct {
	Ctx    context.Context
	Cancel context.CancelFunc
	Env    *envtest.Environment
	Config *rest.Config
	Client client.Client
}

func BootstrapEnvTestSuite(
	ginkgoWriter io.Writer,
	crdDirectoryPath string,
	binaryAssetsBasePath string,
	addToScheme func() error,
) (EnvTestSuiteState, error) {
	logf.SetLogger(zap.New(zap.WriteTo(ginkgoWriter), zap.UseDevMode(true)))

	ctx, cancel := context.WithCancel(context.TODO())
	state := EnvTestSuiteState{
		Ctx:    ctx,
		Cancel: cancel,
	}

	if err := addToScheme(); err != nil {
		cancel()
		return EnvTestSuiteState{}, err
	}

	ginkgo.By("bootstrapping test environment")
	state.Env = &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDirectoryPath},
		ErrorIfCRDPathMissing: true,
	}

	if binaryAssetsDirectory := GetFirstFoundEnvTestBinaryDir(binaryAssetsBasePath); binaryAssetsDirectory != "" {
		state.Env.BinaryAssetsDirectory = binaryAssetsDirectory
	}

	cfg, err := state.Env.Start()
	if err != nil {
		cancel()
		return EnvTestSuiteState{}, err
	}
	state.Config = cfg

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		_ = state.Env.Stop()
		cancel()
		return EnvTestSuiteState{}, err
	}
	state.Client = k8sClient

	return state, nil
}

func TeardownEnvTestSuite(state EnvTestSuiteState) {
	if state.Cancel == nil || state.Env == nil {
		return
	}

	ginkgo.By("tearing down the test environment")
	state.Cancel()
	gomega.Eventually(func() error {
		return state.Env.Stop()
	}, time.Minute, time.Second).Should(gomega.Succeed())
}

func GetFirstFoundEnvTestBinaryDir(basePath string) string {
	entries, err := os.ReadDir(basePath)
	if err != nil {
		logf.Log.Error(err, "Failed to read directory", "path", basePath)
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}
