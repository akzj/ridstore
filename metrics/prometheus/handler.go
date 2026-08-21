// Package prometheus adapts ridstore's bounded Metrics snapshot to the
// Prometheus text exposition format without adding a client-library dependency.
package prometheus

import (
	"bytes"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/akzj/ridstore"
)

const contentType = "text/plain; version=0.0.4; charset=utf-8"

type Source interface {
	Metrics() ridstore.Metrics
}

type Handler struct {
	source Source
	labels string
}

// NewHandler creates a scrape handler. Constant labels identify the embedding
// application or Store; their names and values are validated once and emitted
// on every sample. Paths and other unbounded values should not be labels.
func NewHandler(source Source, constantLabels map[string]string) (*Handler, error) {
	if source == nil {
		return nil, fmt.Errorf("nil metrics source")
	}
	labels, err := encodeLabels(constantLabels)
	if err != nil {
		return nil, err
	}
	return &Handler{source: source, labels: labels}, nil
}

func (h *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if h == nil || h.source == nil {
		http.Error(response, "ridstore metrics source unavailable", http.StatusServiceUnavailable)
		return
	}
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		response.Header().Set("Allow", "GET, HEAD")
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var sampleBuffer [ridstore.MetricSampleCount]ridstore.MetricSample
	samples := h.source.Metrics().AppendMetricSamples(sampleBuffer[:0])
	var body bytes.Buffer
	for _, sample := range samples {
		kind := "gauge"
		if sample.Kind == ridstore.MetricCounter {
			kind = "counter"
		}
		body.WriteString("# TYPE ")
		body.WriteString(sample.Name)
		body.WriteByte(' ')
		body.WriteString(kind)
		body.WriteByte('\n')
		body.WriteString(sample.Name)
		body.WriteString(h.labels)
		body.WriteByte(' ')
		body.WriteString(strconv.FormatUint(sample.Value, 10))
		body.WriteByte('\n')
	}
	response.Header().Set("Content-Type", contentType)
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(http.StatusOK)
	if request.Method == http.MethodGet {
		_, _ = response.Write(body.Bytes())
	}
}

func encodeLabels(labels map[string]string) (string, error) {
	if len(labels) == 0 {
		return "", nil
	}
	names := make([]string, 0, len(labels))
	for name := range labels {
		if !validLabelName(name) || strings.HasPrefix(name, "__") {
			return "", fmt.Errorf("invalid Prometheus label name %q", name)
		}
		if !utf8.ValidString(labels[name]) {
			return "", fmt.Errorf("invalid UTF-8 in Prometheus label %q", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	var encoded strings.Builder
	encoded.WriteByte('{')
	for index, name := range names {
		if index != 0 {
			encoded.WriteByte(',')
		}
		encoded.WriteString(name)
		encoded.WriteString("=\"")
		for _, character := range labels[name] {
			switch character {
			case '\\', '"':
				encoded.WriteByte('\\')
				encoded.WriteRune(character)
			case '\n':
				encoded.WriteString("\\n")
			default:
				encoded.WriteRune(character)
			}
		}
		encoded.WriteByte('"')
	}
	encoded.WriteByte('}')
	return encoded.String(), nil
}

func validLabelName(name string) bool {
	if name == "" || !isLabelFirst(name[0]) {
		return false
	}
	for index := 1; index < len(name); index++ {
		if !isLabelFirst(name[index]) && (name[index] < '0' || name[index] > '9') {
			return false
		}
	}
	return true
}

func isLabelFirst(character byte) bool {
	return character == '_' || character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
}
