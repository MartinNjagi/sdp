package mno_router

import (
	"fmt"
	"sdp/data"
	"strings"
)

// MNORoute is the resolved routing decision for a given MSISDN.
type MNORoute struct {
	// Name is a human-readable MNO label used in logs and metrics.
	// Examples: "safaricom", "airtel", "telkom"
	Name string

	// Dispatcher is the key that maps to a registered Dispatcher implementation.
	// Examples: "http_at", "smpp"
	Dispatcher string

	// TPSLimit is the maximum messages-per-second this MNO allows on our bind.
	// The rate limiter reads this to configure the token bucket.
	TPSLimit int
}

// Router resolves an MSISDN to its MNO route by matching the longest
// matching prefix in the routing table. Prefix matching means a more specific
// entry (e.g. "2547") wins over a catch-all (e.g. "254").
type Router struct {
	// routes is a slice of (prefix, MNORoute) pairs sorted longest-prefix first
	// so the first match wins.
	routes []prefixRoute
}

type prefixRoute struct {
	prefix string
	route  MNORoute
}

// New builds the Router from config.
// cfg.MNORoutes is a slice of RouteConfig populated from environment variables
// or a config file, keeping routing table changes outside the binary.
func New(cfg *data.AppConfig) (*Router, error) {
	if len(cfg.MNORoutes) == 0 {
		return nil, fmt.Errorf("mno_router: no routes configured — set MNO_ROUTES in config")
	}

	routes := make([]prefixRoute, 0, len(cfg.MNORoutes))
	for _, rc := range cfg.MNORoutes {
		if rc.Prefix == "" || rc.Name == "" {
			return nil, fmt.Errorf("mno_router: each route must have a prefix and name")
		}
		if rc.TPSLimit <= 0 {
			rc.TPSLimit = 10 // safe default
		}
		routes = append(routes, prefixRoute{
			prefix: rc.Prefix,
			route: MNORoute{
				Name:       rc.Name,
				Dispatcher: rc.Dispatcher,
				TPSLimit:   rc.TPSLimit,
			},
		})
	}

	// Sort longest prefix first — ensures most-specific match wins.
	sortByPrefixLength(routes)

	return &Router{routes: routes}, nil
}

// Resolve returns the MNORoute for the given MSISDN.
// Returns an error if no configured prefix matches, which the Worker
// treats as a permanent failure (dead-letter the message).
func (r *Router) Resolve(msisdn string) (*MNORoute, error) {
	// Normalise: strip leading '+' so "+2547..." and "2547..." both match.
	msisdn = strings.TrimPrefix(msisdn, "+")

	for _, pr := range r.routes {
		if strings.HasPrefix(msisdn, pr.prefix) {
			route := pr.route // copy — callers must not mutate
			return &route, nil
		}
	}
	return nil, fmt.Errorf("mno_router: no route for msisdn=%s", msisdn)
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// sortByPrefixLength sorts the routes slice in-place, longest prefix first.
func sortByPrefixLength(routes []prefixRoute) {
	// Simple insertion sort — routing tables are tiny (< 100 entries).
	for i := 1; i < len(routes); i++ {
		for j := i; j > 0 && len(routes[j].prefix) > len(routes[j-1].prefix); j-- {
			routes[j], routes[j-1] = routes[j-1], routes[j]
		}
	}
}
