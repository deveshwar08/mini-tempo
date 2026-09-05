package core

// Event represents an immutable activity completion record
type Event struct {
	ActivityName string `json:"activity_name"`
	Payload      []byte `json:"payload"`
	Completed    bool   `json:"completed"`
}

