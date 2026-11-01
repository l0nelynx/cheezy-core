package route

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/metacubex/mihomo/component/powersave"
	"github.com/metacubex/mihomo/constant/features"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

func TestPowerSaveController(t *testing.T) {
	defer powersave.Set(powersave.Active)
	handler := router(false, "test-secret", "", Cors{})
	request := func(method, body, token string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, "/powersave", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if w := request(http.MethodPatch, `{"mode":"idle"}`, "wrong"); w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized PATCH = %d", w.Code)
	}
	for _, mode := range []string{"background", "idle", "active"} {
		w := request(http.MethodPatch, `{"mode":"`+mode+`"}`, "test-secret")
		if !features.WithPowerSave {
			if w.Code != http.StatusNotImplemented || powersave.Current() != powersave.Active {
				t.Fatalf("disabled PATCH = %d, mode %s", w.Code, powersave.Current())
			}
		} else if w.Code != http.StatusNoContent {
			t.Fatalf("PATCH %s = %d: %s", mode, w.Code, w.Body.String())
		}
		w = request(http.MethodGet, "", "test-secret")
		var state struct {
			Enabled bool   `json:"enabled"`
			Mode    string `json:"mode"`
		}
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &state) != nil {
			t.Fatalf("GET = %d: %s", w.Code, w.Body.String())
		}
		want := mode
		if !features.WithPowerSave {
			want = "active"
		}
		if state.Enabled != features.WithPowerSave || state.Mode != want {
			t.Fatalf("GET = %+v, want enabled=%t mode=%s", state, features.WithPowerSave, want)
		}
	}
	if features.WithPowerSave {
		for _, body := range []string{`{}`, `null`, `{"mode":null}`, `{"mode":1}`, `{"mode":"sleep"}`, `{`} {
			w := request(http.MethodPatch, body, "test-secret")
			if w.Code != http.StatusBadRequest || powersave.Current() != powersave.Active {
				t.Fatalf("invalid PATCH %s = %d, mode %s", body, w.Code, powersave.Current())
			}
		}
	}
}
