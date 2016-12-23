package common

type TableStats struct {
	Name   string `json:"name"`
	KeyNum int64  `json:"key_num"`
}

type NamespaceStats struct {
	Name    string       `json:"name"`
	TStats  []TableStats `json:"t_stats"`
	EngType string       `json:"eng_type"`
}

type ServerStats struct {
	// database stats
	NSStats []NamespaceStats `json:"ns_stats"`
	// other server related stats
}
