package externaldatasource

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// terraformCLI shares process setup while keeping each test's provider process
// model, extra environment, and output/state assertions at the call site.
type terraformCLI struct {
	executable string
	dir        string
	env        []string
}

func newTerraformCLI(t *testing.T) *terraformCLI {
	t.Helper()
	executable := os.Getenv("TF_ACC_TERRAFORM_PATH")
	if executable == "" {
		var err error
		executable, err = exec.LookPath("terraform")
		if err != nil {
			t.Skip("Terraform CLI required")
		}
	}
	return &terraformCLI{executable: executable, dir: t.TempDir()}
}

func (cli *terraformCLI) command(ctx context.Context, args ...string) *exec.Cmd {
	//nolint:gosec // Executable is resolved locally; arguments are fixed test subcommands.
	cmd := exec.CommandContext(ctx, cli.executable, args...)
	cmd.Dir = cli.dir
	cmd.Env = append(os.Environ(), "TF_IN_AUTOMATION=1", "CHECKPOINT_DISABLE=1")
	cmd.Env = append(cmd.Env, cli.env...)
	return cmd
}

func (cli *terraformCLI) run(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := cli.command(ctx, args...).CombinedOutput()
	return string(out), err
}

func buildTestProvider(t *testing.T, binary, pkg string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-o", binary, pkg)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}
