package btree

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// Let's start using this very late. Cross-platform support is not exactly my primary goal.
func getPageSize(ctx context.Context) (int, error) {
	cmd := exec.CommandContext(ctx, "getconf", "PAGESIZE")

	var buf bytes.Buffer
	cmd.Stdout = &buf

	if err := cmd.Run(); err != nil {
		return 0, fmt.Errorf("exec: %w", err)
	}

	output := strings.TrimSpace(buf.String())

	n, err := strconv.Atoi(output)
	if err != nil {
		return 0, fmt.Errorf("parse int: %w", err)
	}

	return n, nil
}
