package timezone

import (
	"time"

	"github.com/yoohooshantanu/dialpulse/internal/dnc"
	"github.com/yoohooshantanu/dialpulse/internal/model"
)

// TimezoneGovernor enforces legal outbound dialing windows based on recipient area codes (TCPA 08:00 - 21:00)
type TimezoneGovernor struct {
	areaCodeZones map[string]*time.Location
	defaultLoc    *time.Location
}

func NewTimezoneGovernor() *TimezoneGovernor {
	eastern, _ := time.LoadLocation("America/New_York")
	central, _ := time.LoadLocation("America/Chicago")
	mountain, _ := time.LoadLocation("America/Denver")
	pacific, _ := time.LoadLocation("America/Los_Angeles")
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
	if alaska == nil {
		alaska = time.FixedZone("AKST", -9*3600)
	}
	if hawaii == nil {
		hawaii = time.FixedZone("HST", -10*3600)
	}

	tzMap := map[string]*time.Location{
		// Eastern
		"212": eastern, "347": eastern, "646": eastern, "718": eastern, "917": eastern,
		"202": eastern, "305": eastern, "404": eastern, "617": eastern, "215": eastern,
		// Central
		"312": central, "773": central, "847": central, "214": central, "713": central,
		"615": central, "314": central, "612": central, "504": central,
		// Mountain
		"303": mountain, "720": mountain, "602": mountain, "480": mountain, "801": mountain,
		// Pacific
		"206": pacific, "415": pacific, "650": pacific, "310": pacific, "213": pacific,
		"503": pacific, "858": pacific, "702": pacific,
		// Alaska & Hawaii
		"907": alaska, "808": hawaii,
	}

	return &TimezoneGovernor{
		areaCodeZones: tzMap,
		defaultLoc:    eastern,
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

// CanCall verifies if the recipient is currently within the permissible 08:00 - 21:00 calling window
func (g *TimezoneGovernor) CanCall(phone string, now ...time.Time) error {
	currentTime := time.Now()
	if len(now) > 0 && !now[0].IsZero() {
		currentTime = now[0]
	}

	areaCode := ExtractAreaCode(phone)
	loc, ok := g.areaCodeZones[areaCode]
	if !ok {
		loc = g.defaultLoc
	}

	localTime := currentTime.In(loc)
	hour := localTime.Hour()

	// Legal dialing window: 08:00 AM (inclusive) to 09:00 PM (exclusive)
	if hour < 8 || hour >= 21 {
		return model.ErrTimezoneRestricted
	}

	return nil
}

// GetRecipientTime returns the localized time for a phone number
func (g *TimezoneGovernor) GetRecipientTime(phone string) time.Time {
	areaCode := ExtractAreaCode(phone)
	loc, ok := g.areaCodeZones[areaCode]
	if !ok {
		loc = g.defaultLoc
	}
	return time.Now().In(loc)
}
