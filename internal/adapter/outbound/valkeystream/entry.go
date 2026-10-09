package valkeystream

type Entry struct {
	ID         string
	Fields     map[string]string
	Deliveries int64
}
