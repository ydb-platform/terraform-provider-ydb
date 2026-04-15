package terraform_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAccLocationHostPortFromConn(t *testing.T) {
	tests := []struct {
		conn, want string
	}{
		{"", "localhost:2136"},
		{"grpc://127.0.0.1:2136/?database=/local", "127.0.0.1:2136"},
		{"grpc://localhost:2136/?database=/local", "localhost:2136"},
		{"grpcs://ydb.example.com:2135/?database=/prod", "ydb.example.com:2135"},
	}
	for _, tt := range tests {
		t.Run(tt.conn, func(t *testing.T) {
			assert.Equal(t, tt.want, accLocationHostPortFromConn(tt.conn))
		})
	}
}
