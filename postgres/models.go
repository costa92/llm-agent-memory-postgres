package postgres

import (
	"encoding/json"
)

func encodeJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

func decodeJSON(data []byte, dest any) error {
	return json.Unmarshal(data, dest)
}
