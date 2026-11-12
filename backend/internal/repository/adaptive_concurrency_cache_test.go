package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveConcurrencyCacheReportsAndReads(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	store := NewAdaptiveConcurrencyCache(rdb, service.DefaultAdaptiveConcurrencyParams())
	ctx := context.Background()

	snaps, err := store.GetCwndBatch(ctx, []int64{7})
	require.NoError(t, err)
	require.Zero(t, snaps[7].Window, "未上报过的账号应返回零值快照")

	store.ReportCwnd(ctx, 7, 5, service.CwndOutcomeShed)

	deadline := time.Now().Add(3 * time.Second)
	for {
		snaps, err = store.GetCwndBatch(ctx, []int64{7})
		require.NoError(t, err)
		if wins := snaps[7].Window; wins > 0 && wins < 5 {
			require.InDelta(t, 2.5, wins, 0.01)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("窗口未在超时前收敛: %+v", snaps[7])
		}
		time.Sleep(20 * time.Millisecond)
	}
}
