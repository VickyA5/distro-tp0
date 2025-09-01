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
