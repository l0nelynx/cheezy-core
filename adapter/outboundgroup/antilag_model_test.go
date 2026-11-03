package outboundgroup

import (
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	C "github.com/metacubex/mihomo/constant"
	"github.com/stretchr/testify/require"
)

func TestAntiLagShadowScoresDoNotRoute(t *testing.T) {
	dir, err := filepath.Abs("testdata")
	require.NoError(t, err)
	oldHome := C.Path.HomeDir()
	C.SetHomeDir(dir)
	t.Cleanup(func() { C.SetHomeDir(oldHome) })
	model, err := loadAntiShadow(filepath.Join(dir, "antilag-shadow-v1.json"))
	require.NoError(t, err)
	g, _, _ := labGroup(t, netip.AddrPort{})
	g.shadow = model
	dest := labServer(t, false)
	m := labMeta(dest)
	m.Process = "Darktide.exe"
	pc, err := g.ListenPacketContext(context.Background(), m)
	require.NoError(t, err)
	defer pc.Close()
	exchange(t, pc, dest, "game")
	row := g.Decisions()[0]
	require.Equal(t, "A", row.Selected)
	require.Equal(t, "B", row.ShadowWinner)
	require.Equal(t, model.id, row.ShadowModelID)
	require.Len(t, row.Candidates, 2)
	// Python LightGBM 4.7 on the same synthetic one-tree fixture.
	require.InDelta(t, 1.67, *row.Candidates[0].PredictedStallSeconds, 1e-6)
	require.InDelta(t, 1.53, *row.Candidates[1].PredictedStallSeconds, 1e-6)
	require.Equal(t, "A", g.Bindings()[0].Active)
	require.Equal(t, "B", g.Bindings()[0].ShadowWinner)
	require.Nil(t, model.predict("other.exe", "eu-central-1", antiCandidateInfo{}))
	require.NoError(t, g.Failover(context.Background(), row.Binding, "B"))
	require.Equal(t, "B", g.Bindings()[0].Active)
}
func TestAntiLagRejectsInvalidShadow(t *testing.T) {
	dir := t.TempDir()
	oldHome := C.Path.HomeDir()
	C.SetHomeDir(dir)
	t.Cleanup(func() { C.SetHomeDir(oldHome) })
	original, err := os.ReadFile("testdata/antilag-shadow-v1.json")
	require.NoError(t, err)
	for _, replacement := range [][2]string{{"objective=regression", "objective=binary"}, {"priorCost starts", "starts priorCost"}, {"left_child=-1", "left_child=0"}, {"num_leaves=2", "num_leaves=99999999"}} {
		path := filepath.Join(dir, "bad.json")
		bad := strings.Replace(string(original), replacement[0], replacement[1], 1)
		require.NotEqual(t, string(original), bad)
		require.NoError(t, os.WriteFile(path, []byte(bad), 0600))
		_, err := loadAntiShadow(path)
		require.Error(t, err)
	}
	var absent *antiShadowModel
	require.Nil(t, absent.predict("Darktide.exe", "unknown", antiCandidateInfo{}))
}
