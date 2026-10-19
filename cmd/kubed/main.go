// Command kubed is a k9s-style terminal UI for Kubernetes that also shows
// EKS context: Karpenter node cost, IRSA roles, ExternalSecret sync state
// and ArgoCD ownership.
package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/go-logr/logr"
	"k8s.io/klog/v2"

	"github.com/s3cretagent/kubed/internal/config"
	"github.com/s3cretagent/kubed/internal/ui"
)

// version is set at build time with -ldflags "-X main.version=…".
var version = "dev"

func main() {
	var (
		kubeconfig  = flag.String("kubeconfig", "", "path to kubeconfig (default: $KUBECONFIG or ~/.kube/config)")
		context     = flag.String("context", "", "kubeconfig context (default: current-context)")
		namespace   = flag.String("n", "", "namespace, or all for every namespace (default: context namespace, or all)")
		readOnly    = flag.Bool("readonly", false, "disable every action that changes the cluster")
		mouse       = flag.Bool("mouse", false, "click rows with the mouse (then select text with Option or Shift held); :mouse switches it inside kubed")
		prodPattern = flag.String("prod-pattern", "",
			"regular expression: cluster names that are production, checked before kubed's own rules\n(also $KUBED_PROD_PATTERN, or prodPattern in ~/.config/kubed/config.yaml;\n$KUBED_NONPROD_PATTERN / nonProdPattern for names that are not)")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: kubed [flags] [what to open]\n\nExamples:\n  kubed                      your apps, broken ones first\n  kubed --readonly           look around; nothing can be changed\n  kubed pods                 open straight on a list\n  kubed --context staging -n payments deploy\n\nHelp inside kubed: press ?   Guide: docs/GUIDE.md\n\nFlags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Println("kubed", version)
		return
	}

	cfg, err := config.LoadConfig()
	if err != nil {
		fail(fmt.Errorf("%s/config.yaml: %w", config.Dir(), err))
	}
	// Patterns you set are checked before kubed's own rules (which read
	// the words in a cluster's name: prod, production, staging, dev, qa…).
	pattern, err := optionalPattern(firstSet(*prodPattern, os.Getenv("KUBED_PROD_PATTERN"), cfg.ProdPattern))
	if err != nil {
		fail(fmt.Errorf("prod pattern: %w", err))
	}
	nonProd, err := optionalPattern(firstSet(os.Getenv("KUBED_NONPROD_PATTERN"), cfg.NonProdPattern))
	if err != nil {
		fail(fmt.Errorf("non-prod pattern: %w", err))
	}

	// client-go logs through klog (error-level lines go to stderr even with
	// output redirected). Everything it would say is shown in the UI.
	klog.SetLogger(logr.Discard())

	opts := ui.Options{
		Kubeconfig:     *kubeconfig,
		Context:        *context,
		Namespace:      *namespace,
		Resource:       flag.Arg(0),
		ReadOnly:       *readOnly,
		Mouse:          *mouse,
		ProdPattern:    pattern,
		NonProdPattern: nonProd,
		State:          config.LoadState(),
		Discount:       cfg.Discount,
	}
	m, err := ui.New(opts)
	if err != nil {
		fail(err)
	}

	lines, crashed, restore, err := captureStderr()
	if err == nil {
		m.SetStderr(lines)
		if crashed != "" {
			m.Notice("kubed crashed last time. Details: " + crashed)
		}
	}
	// The mouse is left to the terminal unless asked for, so selecting and
	// copying text works as everywhere else.
	progOpts := []tea.ProgramOption{tea.WithAltScreen()}
	if *mouse {
		progOpts = append(progOpts, tea.WithMouseCellMotion())
	}
	_, runErr := tea.NewProgram(m, progOpts...).Run()
	if restore != nil {
		restore(runErr)
	}
	if runErr != nil {
		fail(runErr)
	}
}

// optionalPattern compiles a pattern you set; "" means none.
func optionalPattern(p string) (*regexp.Regexp, error) {
	if p == "" {
		return nil, nil
	}
	return regexp.Compile(p)
}

// firstSet returns the first non-empty value.
func firstSet(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "kubed:", err)
	os.Exit(1)
}
