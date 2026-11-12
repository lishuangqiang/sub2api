package service

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNextCwndSuccessGrowsAdditively(t *testing.T) {
	params := DefaultAdaptiveConcurrencyParams()
	now := time.Unix(1_000, 0)

	snap := CwndSnapshot{Window: 1, LastUpdateAt: now}
	for i := 0; i < 2; i++ {
		now = now.Add(time.Second)
		snap = NextCwnd(snap, 5, CwndOutcomeSuccess, now, params)
	}
	// w=1 -> +0.5 -> 1.5 -> +0.333 -> 1.833
	require.InDelta(t, 1.833, snap.Window, 0.01)
	require.Equal(t, 1, EffectiveConcurrency(snap, 5))
}

func TestNextCwndShedHalvesAndClamps(t *testing.T) {
	params := DefaultAdaptiveConcurrencyParams()
	now := time.Unix(2_000, 0)

	snap := CwndSnapshot{Window: 5, LastUpdateAt: now}
	snap = NextCwnd(snap, 5, CwndOutcomeShed, now.Add(time.Second), params)
	require.InDelta(t, 2.5, snap.Window, 0.001)
	snap = NextCwnd(snap, 5, CwndOutcomeShed, now.Add(2*time.Second), params)
	require.InDelta(t, 1.25, snap.Window, 0.001)
	snap = NextCwnd(snap, 5, CwndOutcomeShed, now.Add(3*time.Second), params)
	require.InDelta(t, 1, snap.Window, 0.001)
	require.Equal(t, 1, EffectiveConcurrency(snap, 5))
}

func TestNextCwndHolddownBlocksGrowth(t *testing.T) {
	params := DefaultAdaptiveConcurrencyParams()
	now := time.Unix(3_000, 0)

	snap := CwndSnapshot{Window: 2, LastUpdateAt: now}
	snap = NextCwnd(snap, 5, CwndOutcomeShed, now, params) // 2 -> 1, lastShed = now
	require.InDelta(t, 1, snap.Window, 0.001)

	// Holddown 内成功不增长。
	snap = NextCwnd(snap, 5, CwndOutcomeSuccess, now.Add(5*time.Second), params)
	require.InDelta(t, 1, snap.Window, 0.001)

	// Holddown 之后恢复增长。
	snap = NextCwnd(snap, 5, CwndOutcomeSuccess, now.Add(params.Holddown+time.Second), params)
	require.Greater(t, snap.Window, 1.0)
}

func TestNextCwndIdleRecovery(t *testing.T) {
	params := DefaultAdaptiveConcurrencyParams()
	now := time.Unix(4_000, 0)

	snap := CwndSnapshot{Window: 1, LastUpdateAt: now}
	// 两个 IdleReset 周期后应该恢复到 3。
	snap = NextCwnd(snap, 5, CwndOutcomeSuccess, now.Add(2*params.IdleReset+time.Second), params)
	require.InDelta(t, 3.0+0.5/3.0, snap.Window, 0.01)
}

func TestNextCwndTransientAndCap(t *testing.T) {
	params := DefaultAdaptiveConcurrencyParams()
	now := time.Unix(5_000, 0)

	snap := CwndSnapshot{Window: 4, LastUpdateAt: now}
	snap = NextCwnd(snap, 5, CwndOutcomeTransient, now.Add(time.Second), params)
	require.InDelta(t, 2.8, snap.Window, 0.001)

	snap = CwndSnapshot{Window: 5, LastUpdateAt: now}
	snap = NextCwnd(snap, 5, CwndOutcomeSuccess, now.Add(time.Second), params)
	require.LessOrEqual(t, snap.Window, 5.0)
}

func TestEffectiveConcurrencyClamps(t *testing.T) {
	require.Equal(t, 5, EffectiveConcurrency(CwndSnapshot{}, 5))
	require.Equal(t, 1, EffectiveConcurrency(CwndSnapshot{Window: 0.2}, 5))
	require.Equal(t, 5, EffectiveConcurrency(CwndSnapshot{Window: 99}, 5))
	require.Equal(t, 2, EffectiveConcurrency(CwndSnapshot{Window: 2.9}, 5))
	require.Equal(t, 1, EffectiveConcurrency(CwndSnapshot{}, 0))
}

func TestClassifyCwndOutcome(t *testing.T) {
	shed := &UpstreamFailoverError{StatusCode: http.StatusServiceUnavailable, RequestScopedTransient: true}
	require.Equal(t, CwndOutcomeShed, ClassifyCwndOutcome(false, shed))

	fiveHundred := &UpstreamFailoverError{StatusCode: http.StatusInternalServerError}
	require.Equal(t, CwndOutcomeTransient, ClassifyCwndOutcome(false, fiveHundred))

	badRequest := &UpstreamFailoverError{StatusCode: http.StatusBadRequest}
	require.Equal(t, CwndOutcomeNeutral, ClassifyCwndOutcome(false, badRequest))

	require.Equal(t, CwndOutcomeNeutral, ClassifyCwndOutcome(false, context.Canceled))
	require.Equal(t, CwndOutcomeNeutral, ClassifyCwndOutcome(false, fmt.Errorf("boom")))
	require.Equal(t, CwndOutcomeSuccess, ClassifyCwndOutcome(true, nil))
}

func TestFilterByMaxHeadroomPrefersLargerCapacityOnTie(t *testing.T) {
	healthy := &Account{ID: 1, Concurrency: 5}
	flagged := &Account{ID: 2, Concurrency: 5}
	accounts := []accountWithLoad{
		{account: healthy, loadInfo: &AccountLoadInfo{AccountID: 1, CurrentConcurrency: 4}},
		{account: flagged, loadInfo: &AccountLoadInfo{AccountID: 2, CurrentConcurrency: 0}},
	}
	// 尚未建立窗口时，同余量优先容量更大者；这里直接按 headroom 判断。
	require.Len(t, filterByMaxHeadroom(accounts, nil), 1)

	windows := map[int64]int{1: 5, 2: 1}
	result := filterByMaxHeadroom(accounts, windows)
	require.Len(t, result, 1)
	require.Equal(t, int64(1), result[0].account.ID)

	// 健康账号在途 4、窗口 5 => headroom 1；风控账号窗口 1、在途 0 => headroom 1。
	// 余量相同时应优先容量更大的健康账号。
	tieAccounts := []accountWithLoad{
		{account: healthy, loadInfo: &AccountLoadInfo{AccountID: 1, CurrentConcurrency: 4}},
		{account: flagged, loadInfo: &AccountLoadInfo{AccountID: 2, CurrentConcurrency: 0}},
	}
	result = filterByMaxHeadroom(tieAccounts, windows)
	require.Len(t, result, 1)
	require.Equal(t, int64(1), result[0].account.ID)
}
