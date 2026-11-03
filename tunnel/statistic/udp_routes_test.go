package statistic

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
	"github.com/stretchr/testify/require"
)

type mixedRoutes struct{}

func (mixedRoutes) UDPRoutes() []C.UDPRoute {
	return []C.UDPRoute{
		{ID: "one", Chains: C.Chain{"A"}, ProviderChains: C.Chain{"provider"}},
		{ID: "two", Chains: C.Chain{"B"}, ProviderChains: C.Chain{"provider"}},
	}
}

func TestMixedUDPRoutesKeepSeparatePaths(t *testing.T) {
	info := &TrackerInfo{Chain: C.Chain{"game", "vpn"}, ProviderChain: C.Chain{"", ""}, udpRoutes: mixedRoutes{}}
	view := info.snapshot()
	require.Equal(t, C.Chain{"game", "vpn"}, view.Chain)
	require.Equal(t, C.Chain{"A", "game", "vpn"}, view.Routes[0].Chains)
	require.Equal(t, C.Chain{"B", "game", "vpn"}, view.Routes[1].Chains)
	require.Equal(t, C.Chain{"provider", "", ""}, view.Routes[1].ProviderChains)
	require.Empty(t, info.Routes)
}
