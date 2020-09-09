package test

import (
	"os"
	"os/signal"
	"strings"
	"testing"

	configV1 "github.com/openshift/client-go/config/clientset/versioned/typed/config/v1"
	consolev1 "github.com/openshift/client-go/console/clientset/versioned/typed/console/v1"
	routev1 "github.com/openshift/client-go/route/clientset/versioned/typed/route/v1"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/api/client"
	olmversioned "github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/clientset/versioned"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	aggregator "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	operatorversioned "knative.dev/operator/pkg/client/clientset/versioned"
	operatorv1alpha1 "knative.dev/operator/pkg/client/clientset/versioned/typed/operator/v1alpha1"
	servingversioned "knative.dev/serving/pkg/client/clientset/versioned"
)

// Context holds objects related to test execution
type Context struct {
	Name        string
	T           *testing.T
	Clients     *Clients
	CleanupList []CleanupFunc
}

// Clients holds instances of interfaces for making requests to various APIs
type Clients struct {
	Kube               *kubernetes.Clientset
	KubeAggregator     *aggregator.Clientset
	Operator           operatorv1alpha1.OperatorV1alpha1Interface
	Serving            *servingversioned.Clientset
	OLM                olmversioned.Interface
	Dynamic            dynamic.Interface
	Config             *rest.Config
	Route              routev1.RouteV1Interface
	ProxyConfig        configV1.ConfigV1Interface
	ConsoleCLIDownload consolev1.ConsoleCLIDownloadInterface
}

// CleanupFunc defines a function that is called when the respective resource
// should be deleted. When creating resources the user should also create a CleanupFunc
// and register with the Context
type CleanupFunc func() error

var clients []*Clients

// setupClientsOnce creates Clients for all kubeconfigs passed from the command line
func setupClientsOnce(t *testing.T) {
	if len(clients) == 0 {
		kubeconfigs := strings.Split(Flags.Kubeconfigs, ",")
		for _, cfg := range kubeconfigs {
			clientset, err := NewClients(cfg)
			if err != nil {
				t.Fatalf("Couldn't initialize clients for config %s: %v", cfg, err)
			}
			clients = append(clients, clientset)
		}
	}
}

// SetupClusterAdmin returns context for Cluster Admin user
func SetupClusterAdmin(t *testing.T) *Context {
	setupClientsOnce(t)
	return contextAtIndex(0, "ClusterAdmin", t)
}

// SetupProjectAdmin returns context for Project Admin user
func SetupProjectAdmin(t *testing.T) *Context {
	setupClientsOnce(t)
	return contextAtIndex(1, "ProjectAdmin", t)
}

// SetupEdit returns context for user with Edit role
func SetupEdit(t *testing.T) *Context {
	setupClientsOnce(t)
	return contextAtIndex(2, "Edit", t)
}

// SetupView returns context for user with View role
func SetupView(t *testing.T) *Context {
	setupClientsOnce(t)
	return contextAtIndex(3, "View", t)
}

func contextAtIndex(i int, role string, t *testing.T) *Context {
	if len(clients) < i+1 {
		t.Fatalf("kubeconfig for user with %s role not present", role)
	}

	return &Context{
		Name:    role,
		T:       t,
		Clients: clients[i],
	}
}

// NewClients instantiates and returns several clientsets required for making request to the
// Knative cluster
func NewClients(kubeconfig string) (*Clients, error) {
	clients := &Clients{}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}

	// We poll, so set our limits high.
	cfg.QPS = 100
	cfg.Burst = 200

	clients.Kube, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	clients.KubeAggregator, err = aggregator.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	clients.Dynamic, err = dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	clients.Operator, err = newKnativeOperatorClients(cfg)
	if err != nil {
		return nil, err
	}

	clients.Serving, err = servingversioned.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	clients.OLM, err = newOLMClient(kubeconfig)
	if err != nil {
		return nil, err
	}

	clients.Route, err = newOpenShiftRoutes(cfg)
	if err != nil {
		return nil, err
	}

	clients.ProxyConfig, err = newOpenShiftProxyClient(cfg)
	if err != nil {
		return nil, err
	}

	clients.ConsoleCLIDownload, err = newConsoleCLIDownloadClient(cfg)
	if err != nil {
		return nil, err
	}

	clients.Config = cfg
	return clients, nil
}

func newOLMClient(configPath string) (olmversioned.Interface, error) {
	olmclient, err := client.NewClient(configPath)
	if err != nil {
		return nil, err
	}
	return olmclient, nil
}

func newKnativeOperatorClients(cfg *rest.Config) (operatorv1alpha1.OperatorV1alpha1Interface, error) {
	cs, err := operatorversioned.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return cs.OperatorV1alpha1(), nil
}

func newOpenShiftRoutes(cfg *rest.Config) (routev1.RouteV1Interface, error) {
	routeClient, err := routev1.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return routeClient, nil
}

func newOpenShiftProxyClient(cfg *rest.Config) (configV1.ConfigV1Interface, error) {
	proxyClient, err := configV1.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return proxyClient, nil
}

func newConsoleCLIDownloadClient(cfg *rest.Config) (consolev1.ConsoleCLIDownloadInterface, error) {
	consolev1, err := consolev1.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return consolev1.ConsoleCLIDownloads(), nil
}

// Cleanup for all contexts
func CleanupAll(t *testing.T, contexts ...*Context) {
	for _, ctx := range contexts {
		ctx.Cleanup(t)
	}
}

// Cleanup iterates through the list of registered CleanupFunc functions and calls them
func (ctx *Context) Cleanup(t *testing.T) {
	for _, f := range ctx.CleanupList {
		if err := f(); err != nil {
			t.Logf("Failed to clean up: %v", err)
		}
	}
}

// AddToCleanup adds the cleanup function as the first function to the cleanup list,
// we want to delete the last thing first
func (ctx *Context) AddToCleanup(f CleanupFunc) {
	ctx.CleanupList = append([]CleanupFunc{f}, ctx.CleanupList...)
}

// CleanupOnInterrupt will execute the function cleanup if an interrupt signal is caught
func CleanupOnInterrupt(t *testing.T, cleanup func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			t.Logf("Test interrupted, cleaning up.")
			cleanup()
			os.Exit(1)
		}
	}()
}
