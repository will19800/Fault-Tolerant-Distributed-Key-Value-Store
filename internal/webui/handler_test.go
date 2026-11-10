package webui_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/webui"
)

func TestConsoleAndAPIRouting(t *testing.T) {
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	})
	server := httptest.NewServer(webui.New(api))
	defer server.Close()

	for _, test := range []struct {
		path, contentType, contains string
	}{
		{"/", "text/html", "Fault-Tolerant KV"},
		{"/assets/styles.css", "text/css", "--ink"},
		{"/assets/app.js", "text/javascript", "refreshMetrics"},
		{"/healthz", "application/json", `"status":"ok"`},
		{"/metrics", "application/json", `"status":"ok"`},
	} {
		response, err := server.Client().Get(server.URL + test.path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), test.contentType) || !strings.Contains(string(body), test.contains) {
			t.Fatalf("GET %s: status=%d content-type=%q body=%q", test.path, response.StatusCode, response.Header.Get("Content-Type"), body)
		}
	}
	response, err := server.Client().Get(server.URL + "/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("missing status = %d", response.StatusCode)
	}
	if response.Header.Get("Content-Security-Policy") == "" {
		t.Fatal("security headers missing")
	}
}
