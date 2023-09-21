package utils

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd()
	size, err := DirSize(dir)
	assert.Nil(t, err)
	t.Log(size)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	t.Log(size / 1024 / 1024 / 1024)
}
