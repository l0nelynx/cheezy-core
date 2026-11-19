package outboundgroup

import (
	"encoding/json"
	"sort"
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

const antiOutcomeLimit = 512

var outcomeBucket = []byte("antilag_outcomes_v1")
var journalBucket = []byte("antilag_decisions_v1")

type antiJournal struct {
	once          sync.Once
	mu            sync.Mutex
	saveMu        sync.Mutex
	rows          []*antiDecision
	outcomes      []*antiDecision
	outcomesDirty bool
	dirty         bool
}

func (h *antiHistory) loadJournal() {
	if h.db == nil {
		return
	}
	var rows, outcomes []*antiDecision
	var hasArchive bool
	err := h.db.View(func(tx *bbolt.Tx) error {
		if b := tx.Bucket(outcomeBucket); b != nil {
			if data := b.Get(h.key); data != nil {
				hasArchive = true
				if err := json.Unmarshal(data, &outcomes); err != nil {
					return err
				}
			}
		}
		if b := tx.Bucket(journalBucket); b != nil {
			if data := b.Get(h.key); data != nil {
				return json.Unmarshal(data, &rows)
			}
		}
		return nil
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
	// Seed once from completed records still present in a pre-archive cache.
	if !hasArchive {
		for _, row := range h.journal.rows {
			if row.retainOutcome() {
				outcomes = append(outcomes, row)
			}
		}
		sort.SliceStable(outcomes, func(i, j int) bool { return outcomes[i].Ended.Before(*outcomes[j].Ended) })
		h.journal.outcomesDirty = len(outcomes) > 0
	}
	if len(outcomes) > antiOutcomeLimit {
		outcomes = outcomes[len(outcomes)-antiOutcomeLimit:]
	}
	h.journal.outcomes = outcomes
}
func (h *antiHistory) saveJournal() {
	j := &h.journal
	if h.db == nil {
		return
	}
	j.saveMu.Lock()
	defer j.saveMu.Unlock()
	j.mu.Lock()
	if !j.dirty && !j.outcomesDirty {
		j.mu.Unlock()
		return
	}
	rows := decisionCopies(j.rows, false)
	saveOutcomes := j.outcomesDirty
	var outcomes []antiDecision
	if saveOutcomes {
		outcomes = decisionCopies(j.outcomes, false)
	}
	j.dirty, j.outcomesDirty = false, false
	j.mu.Unlock()
	put := func(tx *bbolt.Tx, bucket []byte, data []byte) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		return b.Put(h.key, data)
	}
	recentData, err := json.Marshal(rows)
	var outcomeData []byte
	if err == nil && saveOutcomes {
		outcomeData, err = json.Marshal(outcomes)
	}
	if err == nil {
		err = h.db.Update(func(tx *bbolt.Tx) error {
			if err := put(tx, journalBucket, recentData); err != nil {
				return err
			}
			if saveOutcomes {
				return put(tx, outcomeBucket, outcomeData)
			}
			return nil
		})
	}
	if err != nil {
		j.mu.Lock()
		j.dirty = true
		j.outcomesDirty = j.outcomesDirty || saveOutcomes
		j.mu.Unlock()
		log.Warnln("[AntiLag] saving decision journal: %v", err)
	}
}
func (j *antiJournal) add(row *antiDecision) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.rows = appendDecision(j.rows, row, antiJournalLimit)
	j.retain(row)
	j.dirty = true
}
func appendDecision(rows []*antiDecision, row *antiDecision, limit int) []*antiDecision {
	if len(rows) == limit {
		copy(rows, rows[1:])
		rows = rows[:limit-1]
	}
	return append(rows, row)
}

func (r *antiDecision) retainOutcome() bool {
	return r.Ended != nil && r.Status != "interrupted" && (r.ActiveSeconds >= 20 || r.ConfirmedFailure || (r.Status == "unavailable" && len(r.DialFailures) > 0))
}

// Called under the journal lock when a row is created already final or ends.
func (j *antiJournal) retain(row *antiDecision) {
	if row.retainOutcome() {
		j.outcomes = appendDecision(j.outcomes, row, antiOutcomeLimit)
		j.outcomesDirty = true
	}
}

func decisionCopies(rows []*antiDecision, newestFirst bool) []antiDecision {
	result := make([]antiDecision, len(rows))
	for i, r := range rows {
		target := i
		if newestFirst {
			target = len(rows) - 1 - i
		}
		result[target] = *r
	}
	return result
}

func (g *AntiLag) Decisions() []antiDecision { return g.decisionSnapshot(false) }
func (g *AntiLag) Outcomes() []antiDecision  { return g.decisionSnapshot(true) }

func (g *AntiLag) decisionSnapshot(outcomes bool) []antiDecision {
	j := &g.history.journal
	j.once.Do(g.history.loadJournal)
	j.mu.Lock()
	defer j.mu.Unlock()
	if outcomes {
		return decisionCopies(j.outcomes, true)
	}
	return decisionCopies(j.rows, true)
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
	j.retain(d)
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
