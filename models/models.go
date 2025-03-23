package models

import "encoding/json"

type FirehoseMessage struct {
	RKey   string          `json:"rkey"`
	Seq    int             `json:"seq"`
	Record json.RawMessage `json:"record"`
}
