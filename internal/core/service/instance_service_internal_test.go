package service

import "testing"

// TestNoopLogger only confirms the zero-value fallback never panics — real
// behavior (falling back to it when Log is nil) is exercised via
// test/unit/service's InstanceService tests.
func TestNoopLogger(t *testing.T) {
	var l noopLogger
	l.Debug("msg", nil)
	l.Info("msg", nil)
	l.Warn("msg", nil)
	l.Error("msg", nil)
}
