package common

import "testing"

func TestIsValidNamespace(t *testing.T) {

	tests := []struct {
		name string
		args string
		want bool
	}{
		// TODO: Add test cases.
		{"test", "test", true},
		{"test", "test_$%", false},
		{"test", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidNamespaceName(tt.args); got != tt.want {
				t.Errorf("IsValidTableName() = %v, want %v", got, tt.want)
			}
		})
	}
}
