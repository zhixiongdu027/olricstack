package main

import (
	"flag"
	"log"
	"os"

	olricv1alpha1 "github.com/zhixiongdu/olricstack/api/olric/v1alpha1"
	"github.com/zhixiongdu/olricstack/internal/operator"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func main() {
	var metricsAddr string
	var probeAddr string
	var leaderElect bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint bind address")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8082", "health probe endpoint bind address")
	flag.BoolVar(&leaderElect, "leader-elect", false, "enable leader election")
	flag.Parse()

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(rbacv1.AddToScheme(scheme))
	utilruntime.Must(olricv1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "olricstack.olric.io",
	})
	if err != nil {
		log.Fatalf("create manager: %v", err)
	}

	if err := (&operator.OlricStackReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		log.Fatalf("setup controller: %v", err)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Fatalf("add health check: %v", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Fatalf("add ready check: %v", err)
	}

	log.Printf("starting olricstack operator")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Printf("operator stopped: %v", err)
		os.Exit(1)
	}
}
