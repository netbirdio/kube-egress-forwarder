package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/netbirdio/kube-egress-forwarder/pkg/forwarder"
)

func main() {
	var (
		cmName      string
		cmNamespace string
	)
	flag.StringVar(&cmName, "configmap-name", "", "Name of config map containing rules")
	flag.StringVar(&cmNamespace, "configmap-namespace", "", "Namespace of config map containing rules")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	}))
	slog.SetDefault(logger)

	err := run(cmName, cmNamespace)
	if err != nil {
		slog.Default().Error("exit due to error", "error", err)
		os.Exit(1)
	}
}

func run(cmName, cmNamespace string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)

	fwd := forwarder.NewForwarder(ctx)
	group.Go(func() error {
		return fwd.Wait()
	})

	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	watcher, err := clientset.CoreV1().ConfigMaps(cmNamespace).Watch(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", cmName),
	})
	if err != nil {
		return err
	}
	group.Go(func() error {
		for event := range watcher.ResultChan() {
			cm, ok := event.Object.(*corev1.ConfigMap)
			if !ok {
				continue
			}
			ruleMgr, err := forwarder.NewRuleManager(cm.Data)
			if err != nil {
				return err
			}
			err = fwd.Reconcile(ruleMgr.AllRules())
			if err != nil {
				return err
			}
		}
		return nil
	})

	err = group.Wait()
	if err != nil {
		return err
	}
	return nil
}
