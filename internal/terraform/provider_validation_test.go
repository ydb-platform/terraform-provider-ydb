package terraform

import "testing"

func TestProviderInternalValidate(t *testing.T) {
	if err := Provider().InternalValidate(); err != nil {
		t.Fatalf("provider validation failed: %v", err)
	}
}
