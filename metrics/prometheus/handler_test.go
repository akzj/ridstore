package prometheus

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/akzj/ridstore"
)

type source struct{ metrics ridstore.Metrics }

func (s source) Metrics() ridstore.Metrics { return s.metrics }

func TestHandlerExportsSnapshotAndEscapedSortedLabels(t *testing.T) {
	handler, err := NewHandler(source{metrics: ridstore.Metrics{Committed: 7, DeltaChargedBytes: 11}}, map[string]string{
		"zone": "cn\n1", "app": `a"b\c`,
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != contentType {
		t.Fatalf("status=%d headers=%v", response.Code, response.Header())
	}
	body := response.Body.String()
	if !strings.Contains(body, "# TYPE ridstore_committed_total counter\n") ||
		!strings.Contains(body, `ridstore_committed_total{app="a\"b\\c",zone="cn\n1"} 7`+"\n") ||
		!strings.Contains(body, "ridstore_delta_charged_bytes{app=\"a\\\"b\\\\c\",zone=\"cn\\n1\"} 11\n") {
		t.Fatalf("body=%s", body)
	}
}

func TestHandlerRejectsInvalidLabelsAndMethods(t *testing.T) {
	if _, err := NewHandler(source{}, map[string]string{"bad-label": "x"}); err == nil {
		t.Fatal("expected invalid label error")
	}
	if _, err := NewHandler(source{}, map[string]string{"__reserved": "x"}); err == nil {
		t.Fatal("expected reserved label error")
	}
	if _, err := NewHandler(source{}, map[string]string{"label": string([]byte{0xff})}); err == nil {
		t.Fatal("expected invalid UTF-8 label error")
	}
	handler, err := NewHandler(source{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("status=%d headers=%v", response.Code, response.Header())
	}
}
