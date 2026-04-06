package secret

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resolveSecretValue(ctx context.Context, d *schema.ResourceData) (string, error) {
	if v, ok := d.GetOk("value"); ok {
		return v.(string), nil
	}

	cmdList := d.Get("command").([]interface{})
	if len(cmdList) == 0 {
		return "", fmt.Errorf("either 'value' or 'command' must be specified")
	}

	cmdMap := cmdList[0].(map[string]interface{})
	return runCommand(ctx, cmdMap)
}

func runCommand(ctx context.Context, cmdMap map[string]interface{}) (string, error) {
	path := cmdMap["path"].(string)

	var args []string
	if rawArgs, ok := cmdMap["args"].([]interface{}); ok {
		for _, a := range rawArgs {
			args = append(args, a.(string))
		}
	}

	cmd := exec.CommandContext(ctx, path, args...)

	if rawEnv, ok := cmdMap["env"].(map[string]interface{}); ok {
		for k, v := range rawEnv {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v.(string)))
		}
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("command %q failed: %v\nstdout: %s\nstderr: %s", path, err, stdout.String(), stderr.String())
	}

	return stdout.String(), nil
}
