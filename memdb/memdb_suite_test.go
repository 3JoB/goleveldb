package memdb

import (
	"testing"

	"github.com/3JoB/goleveldb/testutil"
)

func TestMemDB(t *testing.T) {
	testutil.RunSuite(t, "MemDB Suite")
}
