package outboundgroup

import (
	"bufio"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"

	C "github.com/metacubex/mihomo/constant"
	"github.com/vernesong/leaves"
)

// The trainer reads this same list. Every slot comes from the candidate's
// pre-dial API snapshot. Missing observations are encoded as -1 on both sides.
//
//go:embed antilag_features.txt
var antiFeatureSchema string

const shadowFormat = "antilag-stall-seconds-per-active-minute-v1"

type antiShadowModel struct {
	id         string
	process    string
	features   [][]int
	categories []shadowCategory
	ensemble   *leaves.Ensemble
}
type shadowCategory struct {
	Field string `json:"field"`
	Value string `json:"value"`
}

func loadAntiShadow(path string) (model *antiShadowModel, err error) {
	if !C.Path.IsSafePath(path) {
		return nil, C.Path.ErrNotSafePath(path)
	}
	f, err := os.Open(C.Path.Resolve(path))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	const limit = 4 << 20
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, errors.New("shadow model exceeds 4 MiB")
	}
	var spec struct {
		Format     string           `json:"format"`
		Process    string           `json:"process"`
		Categories []shadowCategory `json:"categories"`
		Booster    string           `json:"booster"`
	}
	if err = json.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	if spec.Format != shadowFormat || spec.Process == "" || len(spec.Categories) > 96 {
		return nil, errors.New("incompatible AntiLag model schema")
	}
	sum := sha256.Sum256(data)
	m := &antiShadowModel{id: hex.EncodeToString(sum[:8]), process: spec.Process, categories: spec.Categories}
	var names []string
	for _, key := range strings.Fields(antiFeatureSchema) {
		names = append(names, strings.ReplaceAll(key, ".", "_"))
		typ := reflect.TypeOf(antiCandidateInfo{})
		var indices []int
		for _, part := range strings.Split(key, ".") {
			field, ok := typ.FieldByNameFunc(func(name string) bool {
				f, _ := typ.FieldByName(name)
				tag, _, _ := strings.Cut(f.Tag.Get("json"), ",")
				return tag == part
			})
			if !ok {
				return nil, fmt.Errorf("unknown journal feature %s", key)
			}
			indices = append(indices, field.Index...)
			typ = field.Type
			if typ.Kind() == reflect.Pointer {
				typ = typ.Elem()
			}
		}
		m.features = append(m.features, indices)
	}
	for i, c := range spec.Categories {
		switch c.Field {
		case "node", "destinationRegion", "exitCountry":
		default:
			return nil, fmt.Errorf("unsupported category %q", c.Field)
		}
		names = append(names, fmt.Sprintf("cat_%d", i))
	}
	header, _, ok := strings.Cut(spec.Booster, "\n\n")
	if !ok {
		return nil, errors.New("missing LightGBM header")
	}
	header = "\n" + header + "\n"
	for _, line := range []string{"num_class=1", "objective=regression", "feature_names=" + strings.Join(names, " "), fmt.Sprintf("max_feature_idx=%d", len(names)-1)} {
		if !strings.Contains(header, "\n"+line+"\n") {
			return nil, errors.New("LightGBM feature layout or objective mismatch")
		}
	}
	if err := checkShadowTrees(spec.Booster, len(names)); err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil {
			model = nil
			err = fmt.Errorf("invalid LightGBM tree: %v", r)
		}
	}()
	ensemble, err := leaves.LGEnsembleFromReader(bufio.NewReader(strings.NewReader(spec.Booster)), false)
	if err != nil {
		return nil, err
	}
	if ensemble.NOutputGroups() != 1 || ensemble.NFeatures() != len(names) {
		return nil, errors.New("unexpected model dimensions")
	}
	m.ensemble = ensemble
	return m, nil
}

// The offline trainer emits small numerical trees. Check indices and depth
// before passing external model bytes to the optimized leaves decoder.
func checkShadowTrees(text string, features int) error {
	trees := 0
	for _, block := range strings.Split(text, "\n\n") {
		if !strings.HasPrefix(block, "Tree=") {
			continue
		}
		values := map[string]string{}
		for _, line := range strings.Split(block, "\n") {
			k, v, ok := strings.Cut(line, "=")
			if ok {
				values[k] = v
			}
		}
		trees++
		n, err := strconv.Atoi(values["num_leaves"])
		if err != nil || n < 1 || n > 64 || trees > 256 || values["num_cat"] != "0" || values["is_linear"] == "1" {
			return errors.New("unsupported LightGBM tree")
		}
		for _, key := range []string{"split_feature", "left_child", "right_child"} {
			parts := strings.Fields(values[key])
			if len(parts) != n-1 {
				return fmt.Errorf("invalid %s length", key)
			}
			for i, v := range parts {
				x, err := strconv.Atoi(v)
				if err != nil {
					return err
				}
				if key == "split_feature" {
					if x < 0 || x >= features {
						return errors.New("invalid split feature")
					}
				} else if x < -n || x >= n-1 || (x >= 0 && x <= i) {
					return errors.New("invalid tree child")
				}
			}
		}
	}
	if trees == 0 {
		return errors.New("empty LightGBM model")
	}
	return nil
}

func (m *antiShadowModel) predict(process, region string, c antiCandidateInfo) *float64 {
	if m == nil || !strings.EqualFold(process, m.process) {
		return nil
	}
	// Paths were resolved from journal JSON tags once at load time.
	fields := reflect.ValueOf(c)
	values := make([]float64, 0, len(m.features)+len(m.categories))
	for _, path := range m.features {
		value, err := fields.FieldByIndexErr(path)
		n := -1.0
		if err == nil {
			value = reflect.Indirect(value)
			switch value.Kind() {
			case reflect.Float64:
				n = value.Float()
			case reflect.Uint64:
				n = float64(value.Uint())
			case reflect.Int:
				n = float64(value.Int())
			case reflect.Bool:
				n = 0
				if value.Bool() {
					n = 1
				}
			}
		}
		values = append(values, n)
	}
	for _, cat := range m.categories {
		var s string
		switch cat.Field {
		case "node":
			s = c.Node
		case "exitCountry":
			s = c.ExitCountry
		case "destinationRegion":
			s = region
		}
		n := 0.0
		if s == cat.Value {
			n = 1
		}
		values = append(values, n)
	}
	pred := m.ensemble.PredictSingle(values, 0)
	if math.IsNaN(pred) || math.IsInf(pred, 0) {
		return nil
	}
	pred = math.Min(60, math.Max(0, pred))
	return &pred
}

// Only diagnostics and the decision journal call this. Routing and failover
// never receive model scores, even when the model prefers a different node.
func (g *AntiLag) describeCandidates(m *C.Metadata, list []antiCandidate) ([]antiCandidateInfo, string) {
	result := make([]antiCandidateInfo, 0, len(list))
	best := math.Inf(1)
	winner := ""
	for _, c := range list {
		info := candidateInfo(c)
		info.PredictedStallSeconds = g.shadow.predict(m.Process, m.DestinationRegion, info)
		if p := info.PredictedStallSeconds; p != nil && !c.failed && *p < best {
			winner = c.proxy.Name()
			best = *p
		}
		result = append(result, info)
	}
	return result, winner
}
