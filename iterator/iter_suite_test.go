package iterator_test

import (
	"testing"

	"github.com/3JoB/goleveldb/testutil"
)

func TestIterator(t *testing.T) {
	testutil.RunSuite(t, "Iterator Suite")
}
