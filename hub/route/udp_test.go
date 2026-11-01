package route

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

func TestUDPDiagnosticsController(t *testing.T) {
	statistic.SetUDPDiagnosticsEnabled(false)
	defer statistic.SetUDPDiagnosticsEnabled(false)
	handler := router(false, "test-secret", "", Cors{})
	request := func(method, body, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/udp/diagnostics", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	for _, method := range []string{http.MethodGet, http.MethodPatch} {
		if w := request(method, `{"enabled":true}`, "wrong"); w.Code != http.StatusUnauthorized {
			t.Fatalf("unauthorized %s = %d", method, w.Code)
		}
	}
	if statistic.ActiveUDPDiagnostics() != nil {
		t.Fatal("unauthorized request enabled recording")
	}
	for _, body := range []string{`{}`, `null`, `{"enabled":null}`, `{"enabled":1}`, `{"enabled":"true"}`, `{`} {
		if w := request(http.MethodPatch, body, "test-secret"); w.Code != http.StatusBadRequest {
			t.Fatalf("invalid PATCH %s = %d", body, w.Code)
		}
	}
	if w := request(http.MethodPatch, `{"enabled":true}`, "test-secret"); w.Code != http.StatusNoContent {
		t.Fatalf("enable = %d: %s", w.Code, w.Body.String())
	}
	statistic.ActiveUDPDiagnostics().Drop(statistic.UDPQueueExpired)
	for _, enabled := range []bool{true, false} {
		if !enabled {
			request(http.MethodPatch, `{"enabled":false}`, "test-secret")
		}
		w := request(http.MethodGet, "", "test-secret")
		var snapshot statistic.UDPDiagnosticsSnapshot
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &snapshot) != nil {
			t.Fatalf("GET = %d: %s", w.Code, w.Body.String())
		}
		if snapshot.Enabled != enabled || snapshot.StartedAt == nil || snapshot.Drops["queueExpired"] != 1 {
			t.Fatalf("unexpected snapshot: %+v", snapshot)
		}
	}
}
