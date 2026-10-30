// Command kvctl is the command-line client for the key-value store.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	exitOK       = 0
	exitFailure  = 1
	exitUsage    = 2
	exitNotFound = 3

	defaultServerURL = "http://127.0.0.1:8080"
	defaultTimeout   = 5 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("kvctl", flag.ContinueOnError)
	flags.SetOutput(stderr)
	serverValue := flags.String("server", defaultServerURL, "key-value server base URL")
	timeout := flags.Duration("timeout", defaultTimeout, "request timeout")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			writeUsage(stderr)
			return exitOK
		}
		writeUsage(stderr)
		return exitUsage
	}
	if *timeout <= 0 {
		fmt.Fprintln(stderr, "timeout must be positive")
		return exitUsage
	}

	serverURL, err := normalizeServerURL(*serverValue)
	if err != nil {
		fmt.Fprintf(stderr, "invalid server URL: %v\n", err)
		return exitUsage
	}

	commandArgs := flags.Args()
	if len(commandArgs) == 0 {
		writeUsage(stderr)
		return exitUsage
	}

	client := &http.Client{Timeout: *timeout}
	switch commandArgs[0] {
	case "put":
		if len(commandArgs) != 3 || commandArgs[1] == "" {
			fmt.Fprintln(stderr, "usage: kvctl [flags] put <key> <value>")
			return exitUsage
		}
		return request(ctx, client, http.MethodPut, keyEndpoint(serverURL, commandArgs[1]), strings.NewReader(commandArgs[2]), stdout, stderr)
	case "get":
		if len(commandArgs) != 2 || commandArgs[1] == "" {
			fmt.Fprintln(stderr, "usage: kvctl [flags] get <key>")
			return exitUsage
		}
		return request(ctx, client, http.MethodGet, keyEndpoint(serverURL, commandArgs[1]), nil, stdout, stderr)
	case "delete":
		if len(commandArgs) != 2 || commandArgs[1] == "" {
			fmt.Fprintln(stderr, "usage: kvctl [flags] delete <key>")
			return exitUsage
		}
		return request(ctx, client, http.MethodDelete, keyEndpoint(serverURL, commandArgs[1]), nil, stdout, stderr)
	case "health":
		if len(commandArgs) != 1 {
			fmt.Fprintln(stderr, "usage: kvctl [flags] health")
			return exitUsage
		}
		return request(ctx, client, http.MethodGet, serverURL+"/healthz", nil, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", commandArgs[0])
		writeUsage(stderr)
		return exitUsage
	}
}

func request(ctx context.Context, client *http.Client, method, endpoint string, body io.Reader, stdout, stderr io.Writer) int {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		fmt.Fprintf(stderr, "create request: %v\n", err)
		return exitFailure
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	response, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(stderr, "request failed: %v\n", err)
		return exitFailure
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return handleErrorResponse(response, stderr)
	}
	if _, err := io.Copy(stdout, response.Body); err != nil {
		fmt.Fprintf(stderr, "read response: %v\n", err)
		return exitFailure
	}
	return exitOK
}

func handleErrorResponse(response *http.Response, stderr io.Writer) int {
	var apiError struct {
		Code     string `json:"code"`
		Message  string `json:"message"`
		LeaderID string `json:"leader_id"`
		Outcome  string `json:"outcome"`
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err == nil && json.Unmarshal(body, &apiError) == nil && apiError.Code != "" {
		fmt.Fprintf(stderr, "%s: %s\n", apiError.Code, apiError.Message)
		if apiError.LeaderID != "" {
			fmt.Fprintf(stderr, "known leader: %s\n", apiError.LeaderID)
		}
		if apiError.Outcome == "unknown" {
			fmt.Fprintln(stderr, "the client did not retry because this write may already have committed")
		}
	} else {
		fmt.Fprintf(stderr, "server returned %s\n", response.Status)
	}
	if response.StatusCode == http.StatusNotFound && apiError.Code == "key_not_found" {
		return exitNotFound
	}
	return exitFailure
}

func normalizeServerURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("must use http:// or https:// with a host")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", fmt.Errorf("must not contain credentials, a path, query, or fragment")
	}
	return strings.TrimRight(raw, "/"), nil
}

func keyEndpoint(serverURL, key string) string {
	return serverURL + "/v1/keys/" + url.PathEscape(key)
}

func writeUsage(output io.Writer) {
	var usage bytes.Buffer
	usage.WriteString("usage:\n")
	usage.WriteString("  kvctl [flags] put <key> <value>\n")
	usage.WriteString("  kvctl [flags] get <key>\n")
	usage.WriteString("  kvctl [flags] delete <key>\n")
	usage.WriteString("  kvctl [flags] health\n\n")
	usage.WriteString("exit codes: 0 success, 1 request failure, 2 usage error, 3 key not found\n")
	_, _ = io.Copy(output, &usage)
}
