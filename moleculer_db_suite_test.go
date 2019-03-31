package db

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMoleculerDb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Moleculer DB Unit Tests")
}
