package sqlite_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSqlite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sqlite Integration Test Suite")
}
