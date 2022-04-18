package store_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
)

func TestMoleculerDb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Moleculer DB Integration Tests")
}
