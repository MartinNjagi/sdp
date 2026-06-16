package data

import (
	"fmt"
	"math"
	"strings"
)

// MessageClass mirrors the three lanes the SDP already routes on
// (DispatchEnvelope.MessageType). Kept as a distinct type here so the
// CostEngine's API reads clearly at call sites instead of taking a bare
// string.
type MessageClass string

const (
	ClassVIP      MessageClass = "vip"
	ClassStandard MessageClass = "standard"
	ClassBulk     MessageClass = "bulk"
)

// CostRequest is everything the CostEngine needs to price one message.
// MNO is the carrier name as resolved by mno_router.Router (e.g.
// "safaricom", "airtel") — different carriers may bill segments
// differently, so it is part of the pricing key even though the default
// rate table treats all carriers the same until overridden.
type CostRequest struct {
	Body  string
	Class MessageClass
	MNO   string
}

// CostBreakdown explains how a price was reached — returned alongside the
// final integer cost so callers (logging, client-facing receipts, billing
// disputes) can see the math rather than just the total.
type CostBreakdown struct {
	Segments     int   // Number of SMS segments the body requires
	RatePerPart  int64 // Credits charged per segment for this class/MNO
	TotalCredits int64 // Segments * RatePerPart — what gets deducted
	Class        MessageClass
	MNO          string
}

// RateTable holds the credit cost per segment, keyed by message class and
// then by MNO name. A "default" MNO entry within a class is used when the
// specific carrier has no override — this lets most carriers share a
// price while letting one expensive route (e.g. a premium shortcode)
// diverge without duplicating the whole table.
type RateTable map[MessageClass]map[string]int64

// DefaultRateTable returns sane starting rates: 1 credit per segment for
// every class and carrier. Override individual entries (or load a
// replacement table from config/DB) once real pricing per carrier is
// known — this is intentionally simple so the engine has a working
// default rather than blocking on a pricing spreadsheet.
func DefaultRateTable() RateTable {
	return RateTable{
		ClassVIP:      {"default": 1},
		ClassStandard: {"default": 1},
		ClassBulk:     {"default": 1},
	}
}

// CostEngine prices a compiled message body into an integer credit cost.
// It is the missing piece referenced as "not implemented" in the SDP
// architecture document — DispatchEnvelope.Cost should be set from
// CostEngine.Price(...).TotalCredits before publishing, rather than left
// at its zero-value default.
type CostEngine struct {
	rates       RateTable
	maxSegments int // 0 = unlimited; otherwise segment counts above this are rejected
}

// NewCostEngine constructs a CostEngine. Pass DefaultRateTable() to start,
// or a custom table loaded from config/DB. maxSegments caps how many parts
// a single message may split into before Price returns an error — set to
// 0 to disable the cap.
func NewCostEngine(rates RateTable, maxSegments int) *CostEngine {
	if rates == nil {
		rates = DefaultRateTable()
	}
	return &CostEngine{rates: rates, maxSegments: maxSegments}
}

// Price computes the segment count for req.Body and multiplies it by the
// configured per-segment rate for req.Class/req.MNO. Returns an error if
// maxSegments is set and exceeded — callers should treat that as a
// permanent failure (reject before queuing), not something to retry.
func (c *CostEngine) Price(req CostRequest) (*CostBreakdown, error) {
	segments := GetNumberSMSSegments(req.Body, c.maxSegments)

	if c.maxSegments > 0 && segments > c.maxSegments {
		return nil, fmt.Errorf(
			"cost engine: message requires %d segments, exceeds max of %d",
			segments, c.maxSegments,
		)
	}

	rate := c.rateFor(req.Class, req.MNO)
	total := int64(segments) * rate

	return &CostBreakdown{
		Segments:     segments,
		RatePerPart:  rate,
		TotalCredits: total,
		Class:        req.Class,
		MNO:          req.MNO,
	}, nil
}

// rateFor looks up the per-segment rate for a class/MNO pair, falling back
// to the class's "default" entry, and finally to 1 credit if the class
// itself is entirely unconfigured (defensive — should not happen with
// DefaultRateTable, but avoids a silent zero-cost message if a custom
// table is incomplete).
func (c *CostEngine) rateFor(class MessageClass, mno string) int64 {
	classRates, ok := c.rates[class]
	if !ok {
		return 1
	}
	if rate, ok := classRates[mno]; ok {
		return rate
	}
	if rate, ok := classRates["default"]; ok {
		return rate
	}
	return 1
}

// SetRate overrides the per-segment rate for a specific class/MNO pair at
// runtime — e.g. an ops endpoint that adjusts pricing without a redeploy.
func (c *CostEngine) SetRate(class MessageClass, mno string, creditsPerSegment int64) {
	if c.rates[class] == nil {
		c.rates[class] = make(map[string]int64)
	}
	c.rates[class][mno] = creditsPerSegment
}

// GetNumberSMSSegments calculates the number of SMS segments needed for a given text.
func GetNumberSMSSegments(text string, MaxSegments int) int {
	if len(text) == 0 {
		return 0 // Empty SMS check
	}

	var SingleMax, ConcatMax int
	if isGsm7bit(text) { // 7-bit encoding
		SingleMax = 160
		ConcatMax = 153
	} else { // UCS-2 Encoding (16-bit)
		SingleMax = 70
		ConcatMax = 67
	}

	textlen := runeCount(text)
	var TotalSegment int

	if textlen <= SingleMax {
		TotalSegment = 1
	} else {
		TotalSegment = int(math.Ceil(float64(textlen) / float64(ConcatMax)))
	}

	return TotalSegment
}

// runeCount returns the number of runes in a string.
func runeCount(text string) int {
	count := 0
	for range text {
		count++
	}
	return count
}

func isGsm7bit(text string) bool {
	gsm7bitChars := "\\@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&amp;'()*+,-./0123456789:;<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà^{}[~]|€"
	for _, char := range text {
		if !strings.ContainsRune(gsm7bitChars, char) && char != '\\' {
			return false
		}
	}
	return true
}
