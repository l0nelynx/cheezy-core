package outboundgroup

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/log"
)

// One decision per UDP path generation. Features are captured before dialing;
// outcomes are accumulated only on the path that actually carried traffic.
type antiDecision struct {
	ShadowWinner       string              `json:"shadowWinner,omitempty"`
	ShadowModelID      string              `json:"shadowModelId,omitempty"`
	Binding            string              `json:"binding"`
	Started            time.Time           `json:"startedAt"`
	Destination        string              `json:"destination"`
	DestinationRegion  string              `json:"destinationRegion"`
	DestinationCountry string              `json:"destinationCountry,omitempty"`
	GeoSource          string              `json:"geoSource"`
	ASN                string              `json:"asn,omitempty"`
	Process            string              `json:"process,omitempty"`
	Trigger            string              `json:"trigger"`
	Candidates         []antiCandidateInfo `json:"candidates"`
	Selected           string              `json:"selected,omitempty"`
	Selection          string              `json:"selection,omitempty"`
	DialFailures       []string            `json:"failedCandidates,omitempty"`
	Status             string              `json:"status"`
	Ended              *time.Time          `json:"endedAt,omitempty"`
	FirstResponseMS    *float64            `json:"firstResponseMs,omitempty"`
	ActiveSeconds      float64             `json:"activeSeconds"`
	ExcessSeconds      float64             `json:"excessSeconds"`
	Events             uint64              `json:"events"`
	ConfirmedFailure   bool                `json:"confirmedFailure"`
}

const antiJournalLimit = 256

var journalBucket = []byte("antilag_decisions_v1")

type antiJournal struct {
	once   sync.Once
	mu     sync.Mutex
	saveMu sync.Mutex
	rows   []*antiDecision
	dirty  bool
}

func (h *antiHistory) loadJournal() {
	if h.db == nil {
		return
	}
	var rows []*antiDecision
	err := h.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(journalBucket)
		if b == nil {
			return nil
		}
		data := b.Get(h.key)
		if len(data) == 0 {
			return nil
		}
		return json.Unmarshal(data, &rows)
	})
	if err != nil {
		log.Warnln("[AntiLag] loading decision journal: %v", err)
		return
	}
	if len(rows) > antiJournalLimit {
		rows = rows[len(rows)-antiJournalLimit:]
	}
	h.journal.mu.Lock()
	defer h.journal.mu.Unlock()
	for _, r := range rows {
		if r == nil {
			continue
		}
		if r.Status == "active" {
			r.Status = "interrupted"
			h.journal.dirty = true
		}
		h.journal.rows = append(h.journal.rows, r)
	}
}
func (h *antiHistory) saveJournal() {
	j := &h.journal
	if h.db == nil {
		return
	}
	j.saveMu.Lock()
	defer j.saveMu.Unlock()
	j.mu.Lock()
	if !j.dirty {
		j.mu.Unlock()
		return
	}
	rows := make([]antiDecision, len(j.rows))
	for i, r := range j.rows {
		rows[i] = *r
	}
	j.dirty = false
	j.mu.Unlock()
	data, err := json.Marshal(rows)
	if err == nil {
		err = h.db.Update(func(tx *bbolt.Tx) error {
			b, e := tx.CreateBucketIfNotExists(journalBucket)
			if e != nil {
				return e
			}
			return b.Put(h.key, data)
		})
	}
	if err != nil {
		j.mu.Lock()
		j.dirty = true
		j.mu.Unlock()
		log.Warnln("[AntiLag] saving decision journal: %v", err)
	}
}
func (j *antiJournal) add(row *antiDecision) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.rows) == antiJournalLimit {
		copy(j.rows, j.rows[1:])
		j.rows = j.rows[:antiJournalLimit-1]
	}
	j.rows = append(j.rows, row)
	j.dirty = true
}
func (g *AntiLag) Decisions() []antiDecision {
	j := &g.history.journal
	j.once.Do(g.history.loadJournal)
	j.mu.Lock()
	defer j.mu.Unlock()
	result := make([]antiDecision, 0, len(j.rows))
	for i := len(j.rows) - 1; i >= 0; i-- {
		result = append(result, *j.rows[i])
	}
	return result
}
func (j *antiJournal) end(p *antiPath, status string, failed bool) {
	if p == nil || p.decision == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	d := p.decision
	if d.Status != "active" {
		return
	}
	now := time.Now()
	d.Ended = &now
	d.Status = status
	d.ConfirmedFailure = failed
	j.dirty = true
}
func (j *antiJournal) stalls(p *antiPath, d stallDelta) {
	if p.decision == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	p.decision.ActiveSeconds += d.Active.Seconds()
	p.decision.ExcessSeconds += d.Excess.Seconds()
	p.decision.Events += d.Events
	j.dirty = true
}
func (j *antiJournal) reply(p *antiPath, delay time.Duration) {
	if p.decision == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	ms := float64(delay) / float64(time.Millisecond)
	p.decision.FirstResponseMS = &ms
	j.dirty = true
}
