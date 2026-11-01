package route

import (
	"github.com/metacubex/mihomo/component/powersave"
	"github.com/metacubex/mihomo/constant/features"

	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func getPowerSave(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, map[string]any{
		"enabled": features.WithPowerSave,
		"mode":    powersave.Current().String(),
	})
}

func patchPowerSave(w http.ResponseWriter, r *http.Request) {
	if !features.WithPowerSave {
		render.Status(r, http.StatusNotImplemented)
		render.JSON(w, r, newError("core was built without with_powersave"))
		return
	}
	var request struct {
		Mode *powersave.Mode `json:"mode"`
	}
	if err := render.DecodeJSON(r.Body, &request); err != nil || request.Mode == nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("mode must be active, background or idle"))
		return
	}
	powersave.Set(*request.Mode)
	render.NoContent(w, r)
}
