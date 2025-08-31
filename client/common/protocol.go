package common

import "fmt"

type Bet struct {
	Agency    string
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    string
}

type Protocol struct{}

func (Protocol) escape(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == '\\' || r == '|' {
			out = append(out, '\\')
		}
		out = append(out, r)
	}
	return string(out)
}

func (p Protocol) SerializeBet(b Bet) string {
	return fmt.Sprintf("BET|%s|%s|%s|%s|%s|%s\n",
		p.escape(b.Agency),
		p.escape(b.FirstName),
		p.escape(b.LastName),
		p.escape(b.Document),
		p.escape(b.Birthdate),
		p.escape(b.Number),
	)
}
