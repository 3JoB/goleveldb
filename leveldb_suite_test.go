package leveldb

import (
	"testing"

	"github.com/3JoB/goleveldb/testutil"
)

func TestLevelDB(t *testing.T) {
	testutil.RunSuite(t, "LevelDB Suite")
}
