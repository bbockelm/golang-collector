package accountant

import (
	"strconv"
	"strings"
	"time"
)

// Knob parsing: build accountant / group configurations from HTCondor
// configuration values. The getter abstracts the config source (typically
// (*config.Config).Get from golang-htcondor) so this package stays free of a
// config dependency and tests can inject maps. Unset or unparsable knobs keep
// their HTCondor defaults (design doc section 9).

// KnobGetter returns the raw value of an HTCondor configuration knob.
type KnobGetter func(key string) (string, bool)

// ConfigFromKnobs reads the accountant's knobs: PRIORITY_HALFLIFE,
// DEFAULT_PRIO_FACTOR, NICE_USER_PRIO_FACTOR, REMOTE_PRIO_FACTOR,
// ACCOUNTANT_LOCAL_DOMAIN, NEGOTIATOR_USE_SLOT_WEIGHTS,
// NEGOTIATOR_DISCOUNT_SUSPENDED_RESOURCES, and the GROUP_PRIO_FACTOR_<group>
// hook. LogFile (ACCOUNTANT_DATABASE_FILE) is left to the caller: its default
// involves $(SPOOL) and a deliberate Go-native filename (see the mains).
func ConfigFromKnobs(get KnobGetter) Config {
	cfg := DefaultConfig()
	if v, ok := knobFloat(get, "PRIORITY_HALFLIFE"); ok && v > 0 {
		cfg.HalfLife = time.Duration(v * float64(time.Second))
	}
	if v, ok := knobFloat(get, "DEFAULT_PRIO_FACTOR"); ok && v > 0 {
		cfg.DefaultPrioFactor = v
	}
	if v, ok := knobFloat(get, "NICE_USER_PRIO_FACTOR"); ok && v > 0 {
		cfg.NiceUserPrioFactor = v
	}
	if v, ok := knobFloat(get, "REMOTE_PRIO_FACTOR"); ok && v > 0 {
		cfg.RemotePrioFactor = v
	}
	if v, ok := get("ACCOUNTANT_LOCAL_DOMAIN"); ok {
		cfg.LocalDomain = strings.TrimSpace(v)
	}
	cfg.UseSlotWeights = knobBool(get, "NEGOTIATOR_USE_SLOT_WEIGHTS", true)
	cfg.DiscountSuspended = knobBool(get, "NEGOTIATOR_DISCOUNT_SUSPENDED_RESOURCES", false)
	cfg.GroupPrioFactor = func(group string) float64 {
		v, _ := knobFloat(get, "GROUP_PRIO_FACTOR_"+strings.ToUpper(group))
		return v
	}
	return cfg
}

// GroupConfigFromKnobs reads the accounting-group / hierarchical-quota knobs:
// GROUP_NAMES, GROUP_QUOTA_<g> / GROUP_QUOTA_DYNAMIC_<g>,
// GROUP_ACCEPT_SURPLUS[_<g>], GROUP_AUTOREGROUP[_<g>], GROUP_SORT_EXPR,
// NEGOTIATOR_ALLOW_QUOTA_OVERSUBSCRIPTION, NEGOTIATOR_STRICT_ENFORCE_QUOTA,
// GROUP_QUOTA_MAX_ALLOCATION_ROUNDS, NEGOTIATOR_USE_WEIGHTED_DEMAND, and
// GROUP_QUOTA_ROUND_ROBIN_RATE. An unset GROUP_NAMES yields the flat
// (single-root) pool.
func GroupConfigFromKnobs(get KnobGetter) GroupConfig {
	cfg := DefaultGroupConfig()
	if v, ok := get("GROUP_NAMES"); ok {
		cfg.GroupNames = splitKnobList(v)
	}
	for _, g := range cfg.GroupNames {
		key := strings.ToUpper(g)
		if v, ok := knobFloat(get, "GROUP_QUOTA_"+key); ok {
			cfg.GroupQuota[g] = v
		} else if v, ok := knobFloat(get, "GROUP_QUOTA_DYNAMIC_"+key); ok {
			cfg.GroupQuotaDynamic[g] = v
		}
		if v, ok := get("GROUP_ACCEPT_SURPLUS_" + key); ok {
			cfg.GroupAcceptSurplus[g] = parseKnobBool(v, false)
		}
		if v, ok := get("GROUP_AUTOREGROUP_" + key); ok {
			cfg.GroupAutoregroup[g] = parseKnobBool(v, false)
		}
	}
	cfg.DefaultAcceptSurplus = knobBool(get, "GROUP_ACCEPT_SURPLUS", false)
	cfg.DefaultAutoregroup = knobBool(get, "GROUP_AUTOREGROUP", false)
	if v, ok := get("GROUP_SORT_EXPR"); ok && strings.TrimSpace(v) != "" {
		cfg.GroupSortExpr = strings.TrimSpace(v)
	}
	cfg.AllowQuotaOversubscription = knobBool(get, "NEGOTIATOR_ALLOW_QUOTA_OVERSUBSCRIPTION", false)
	cfg.StrictEnforceQuota = knobBool(get, "NEGOTIATOR_STRICT_ENFORCE_QUOTA", true)
	if v, ok := knobInt(get, "GROUP_QUOTA_MAX_ALLOCATION_ROUNDS"); ok && v > 0 {
		cfg.MaxAllocationRounds = v
	}
	cfg.UseWeightedDemand = knobBool(get, "NEGOTIATOR_USE_WEIGHTED_DEMAND", true)
	if v, ok := knobFloat(get, "GROUP_QUOTA_ROUND_ROBIN_RATE"); ok && v >= 1 {
		cfg.RoundRobinRate = v
	}
	return cfg
}

// splitKnobList splits an HTCondor list knob on commas and whitespace.
func splitKnobList(v string) []string {
	var out []string
	for _, s := range strings.FieldsFunc(v, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	}) {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// knobBool reads an HTCondor boolean knob (true/t/yes/1, case-insensitive),
// returning def when unset or unrecognized.
func knobBool(get KnobGetter, key string, def bool) bool {
	v, ok := get(key)
	if !ok {
		return def
	}
	return parseKnobBool(v, def)
}

func parseKnobBool(v string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "t", "yes", "y", "1":
		return true
	case "false", "f", "no", "n", "0":
		return false
	}
	return def
}

func knobFloat(get KnobGetter, key string) (float64, bool) {
	v, ok := get(key)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func knobInt(get KnobGetter, key string) (int, bool) {
	v, ok := get(key)
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, false
	}
	return n, true
}
