package timezone

import (
	"time"

	"github.com/yoohooshantanu/predictive-dialer/internal/dnc"
	"github.com/yoohooshantanu/predictive-dialer/internal/model"
)

// TimezoneGovernor enforces legal outbound dialing windows based on recipient area codes (TCPA 08:00 - 21:00)
type TimezoneGovernor struct {
	areaCodeZones map[string][]*time.Location
	defaultLoc    *time.Location
}

func NewTimezoneGovernor() *TimezoneGovernor {
	eastern, _ := time.LoadLocation("America/New_York")
	central, _ := time.LoadLocation("America/Chicago")
	mountain, _ := time.LoadLocation("America/Denver")
	pacific, _ := time.LoadLocation("America/Los_Angeles")
	phoenix, _ := time.LoadLocation("America/Phoenix")
	alaska, _ := time.LoadLocation("America/Anchorage")
	hawaii, _ := time.LoadLocation("Pacific/Honolulu")

	// If system tzdata is unavailable, fallback to fixed UTC offsets
	if eastern == nil {
		eastern = time.FixedZone("EST", -5*3600)
	}
	if central == nil {
		central = time.FixedZone("CST", -6*3600)
	}
	if mountain == nil {
		mountain = time.FixedZone("MST", -7*3600)
	}
	if pacific == nil {
		pacific = time.FixedZone("PST", -8*3600)
	}
	if phoenix == nil {
		phoenix = time.FixedZone("MST", -7*3600) // Arizona does not observe DST; remains UTC-7 year-round
	}
	if alaska == nil {
		alaska = time.FixedZone("AKST", -9*3600)
	}
	if hawaii == nil {
		hawaii = time.FixedZone("HST", -10*3600)
	}

	tzMap := map[string][]*time.Location{
		// Eastern
		"212": {eastern}, "347": {eastern}, "646": {eastern}, "718": {eastern}, "917": {eastern},
		"202": {eastern}, "305": {eastern}, "404": {eastern}, "617": {eastern}, "215": {eastern},
		// Central
		"312": {central}, "773": {central}, "847": {central}, "214": {central}, "713": {central},
		"615": {central}, "314": {central}, "612": {central}, "504": {central},
		// Mountain (Denver observes DST)
		"303": {mountain}, "720": {mountain}, "801": {mountain}, "915": {mountain},
		// Arizona non-DST (America/Phoenix is UTC-7 year-round)
		"602": {phoenix}, "480": {phoenix}, "623": {phoenix},
		// Area code 928 covers both the Navajo Nation (which observes DST / Mountain Time)
		// and Hopi / rural Arizona (which does not observe DST / Phoenix Time).
		// PredictiveDialer enforces the conservative intersection: dials are permitted only when
		// BOTH Phoenix and Navajo (Denver) local times fall within 08:00 - 21:00.
		"928": {phoenix, mountain},
		// Pacific
		"206": {pacific}, "415": {pacific}, "650": {pacific}, "310": {pacific}, "213": {pacific},
		"503": {pacific}, "858": {pacific}, "702": {pacific},
		// Alaska & Hawaii
		"907": {alaska}, "808": {hawaii},
	}

	return &TimezoneGovernor{
		areaCodeZones: tzMap,
		defaultLoc:    nil, // Fail closed: unrecognized area codes return model.ErrUnknownTimezone
	}
}

// ExtractAreaCode extracts the 3-digit NPA area code from a phone number
func ExtractAreaCode(phone string) string {
	digits := dnc.NormalizeNumber(phone)
	if len(digits) >= 10 {
		return digits[:3]
	}
	return ""
}

// CanCall verifies if the recipient is currently within the permissible 08:00 - 21:00 calling window.
// For multi-jurisdiction area codes (like 928), ALL applicable zones must be within the legal window (intersection).
func (g *TimezoneGovernor) CanCall(phone string, now ...time.Time) error {
	currentTime := time.Now()
	if len(now) > 0 && !now[0].IsZero() {
		currentTime = now[0]
	}

	areaCode := ExtractAreaCode(phone)
	locs, ok := g.areaCodeZones[areaCode]
	if !ok || len(locs) == 0 {
		return model.ErrUnknownTimezone
	}

	for _, loc := range locs {
		localTime := currentTime.In(loc)
		hour := localTime.Hour()

		// Legal dialing window: 08:00 AM (inclusive) to 09:00 PM (exclusive)
		if hour < 8 || hour >= 21 {
			return model.ErrTimezoneRestricted
		}
	}

	return nil
}

// GetRecipientTime returns the localized time for a phone number
func (g *TimezoneGovernor) GetRecipientTime(phone string) time.Time {
	areaCode := ExtractAreaCode(phone)
	locs, ok := g.areaCodeZones[areaCode]
	if !ok || len(locs) == 0 {
		return time.Time{}
	}
	return time.Now().In(locs[0])
}

// NextPermissibleCallTime computes the exact timestamp when dialing becomes legally permissible (latest valid window)
func (g *TimezoneGovernor) NextPermissibleCallTime(phone string, now ...time.Time) time.Time {
	currentTime := time.Now()
	if len(now) > 0 && !now[0].IsZero() {
		currentTime = now[0]
	}

	areaCode := ExtractAreaCode(phone)
	locs, ok := g.areaCodeZones[areaCode]
	if !ok || len(locs) == 0 {
		return time.Time{}
	}

	var latest time.Time
	for _, loc := range locs {
		localTime := currentTime.In(loc)
		hour := localTime.Hour()

		var target time.Time
		if hour < 8 {
			target = time.Date(localTime.Year(), localTime.Month(), localTime.Day(), 8, 0, 0, 0, loc).UTC()
		} else if hour >= 21 {
			tomorrow := localTime.AddDate(0, 0, 1)
			target = time.Date(tomorrow.Year(), tomorrow.Month(), tomorrow.Day(), 8, 0, 0, 0, loc).UTC()
		} else {
			target = currentTime.UTC()
		}

		if target.After(latest) {
			latest = target
		}
	}

	return latest
}
