package outboundgroup

import (
	"encoding/json"
	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/log"
	"net/netip"
	"time"
)

type savedProbe struct {
	At    time.Time
	RTT   time.Duration
	Reply bool
}
type savedRegion struct {
	Winner string
	Nodes  map[string][]savedProbe
}

var probeBucket = []byte("antilag_probes_v1")

func (g *AntiLag) saveRegions() {
	if g.history.db == nil || len(g.targets) == 0 {
		return
	}
	data := map[netip.AddrPort]savedRegion{}
	g.mu.Lock()
	for target, r := range g.measurements {
		region := savedRegion{Winner: r.winner, Nodes: map[string][]savedProbe{}}
		for name, window := range r.samples {
			for _, s := range window {
				if time.Since(s.at) <= 5*time.Minute {
					region.Nodes[name] = append(region.Nodes[name], savedProbe{s.at, s.duration, s.ok})
				}
			}
			if len(region.Nodes[name]) == 0 {
				delete(r.samples, name)
			}
		}
		if len(region.Nodes) > 0 {
			data[target] = region
		}
	}
	g.mu.Unlock()
	if len(data) == 0 {
		return
	}
	raw, err := json.Marshal(data)
	if err == nil {
		err = g.history.db.Update(func(tx *bbolt.Tx) error {
			b, e := tx.CreateBucketIfNotExists(probeBucket)
			if e != nil {
				return e
			}
			return b.Put(g.history.key, raw)
		})
	}
	if err != nil {
		log.Warnln("[AntiLag] saving probe history: %v", err)
	}
}
func (g *AntiLag) loadRegions() {
	if g.history.db == nil || len(g.targets) == 0 {
		return
	}
	var data map[netip.AddrPort]savedRegion
	err := g.history.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(probeBucket)
		if b == nil {
			return nil
		}
		raw := b.Get(g.history.key)
		if raw == nil {
			return nil
		}
		return json.Unmarshal(raw, &data)
	})
	if err != nil {
		log.Warnln("[AntiLag] reading probe history: %v", err)
		return
	}
	for _, t := range g.targets {
		region, ok := data[t.endpoint]
		if !ok {
			continue
		}
		r := &antiRegion{winner: region.Winner, samples: map[string][]probeSample{}}
		for name, v := range region.Nodes {
			for _, s := range v {
				if age := time.Since(s.At); age >= 0 && age <= 5*time.Minute {
					r.record(name, probeSample{s.At, s.RTT, s.Reply})
				}
			}
		}
		if len(r.samples) > 0 {
			g.measurements[t.endpoint] = r
		}
	}
}
