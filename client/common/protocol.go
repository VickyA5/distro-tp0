package common

import "fmt"

// Bet represents a lottery bet with all required participant information
type Bet struct {
	Agency    string // Lottery agency identifier
	FirstName string // Participant's first name
	LastName  string // Participant's last name
	Document  string // Participant's document number
	Birthdate string // Participant's birthdate in YYYY-MM-DD format
	Number    string // Lottery number being bet on
}

// Protocol handles BET message serialization for client-server communication
type Protocol struct{}

// escape protects special characters in strings for safe transmission.
// It escapes backslashes and hash symbols to prevent parsing conflicts.
func (Protocol) escape(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == '\\' || r == '#' {
			out = append(out, '\\')
		}
		out = append(out, r)
	}
	return string(out)
}

// SerializeBet converts a Bet struct into a formatted BET message string.
// The message format is: BET#agency#first_name#last_name#document#birthdate#number
// All fields are properly escaped to handle special characters safely.
func (p Protocol) SerializeBet(b Bet) string {
	return fmt.Sprintf("BET#%s#%s#%s#%s#%s#%s\n",
		p.escape(b.Agency),
		p.escape(b.FirstName),
		p.escape(b.LastName),
		p.escape(b.Document),
		p.escape(b.Birthdate),
		p.escape(b.Number),
	)
}

// SerializeBatch converts a slice of Bet structs into a formatted BATCH message string.
// The message format is: BATCH#count\nBET#agency#first_name#last_name#document#birthdate#number\n...
// This allows sending multiple bets in a single transmission for efficient batch processing.
func (p Protocol) SerializeBatch(bets []Bet) string {
	if len(bets) == 0 {
		return ""
	}
	
	result := fmt.Sprintf("BATCH#%d\n", len(bets))
	for _, bet := range bets {
		betStr := p.SerializeBet(bet)
		result += betStr
	}
	return result
}

// SerializeFinishBets creates a FINISH_BETS message to notify the server
// that the agency has finished sending all its bets
func (p Protocol) SerializeFinishBets(agency string) string {
	return fmt.Sprintf("FINISH_BETS#%s\n", p.escape(agency))
}

// SerializeQueryWinners creates a QUERY_WINNERS message to request
// the list of winners for a specific agency
func (p Protocol) SerializeQueryWinners(agency string) string {
	return fmt.Sprintf("QUERY_WINNERS#%s\n", p.escape(agency))
}
