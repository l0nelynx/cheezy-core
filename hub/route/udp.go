package route

import (
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func getUDPDiagnostics(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, statistic.SnapshotUDPDiagnostics())
}

func patchUDPDiagnostics(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Enabled *bool `json:"enabled"`
	}
	if err := render.DecodeJSON(r.Body, &request); err != nil || request.Enabled == nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("enabled must be a boolean"))
		return
	}
	statistic.SetUDPDiagnosticsEnabled(*request.Enabled)
	render.NoContent(w, r)
}
