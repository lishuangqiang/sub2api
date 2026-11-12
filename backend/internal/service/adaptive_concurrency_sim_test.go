package service

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 这是一个离线仿真：5 个账号，其中 3 个被上游风控限流到并发 1，
// 对比「现有调度（按负载率选号 + 同账号重试 3 次）」与
// 「自适应窗口调度（按可用余量选号 + 容量拒绝不重试）」的表现。

type simScheduler int

const (
	simBaseline simScheduler = iota
	simAIMD
)

type simAccount struct {
	id          int64
	upstreamCap int
	configured  int
	inflight    int
	completions []float64
	window      float64
	lastShed    time.Time
	lastUpdate  time.Time
	attempts    int
	sheds       int
	flagged     bool
}

type simPending struct {
	deadline float64
}

type simResult struct {
	requests        int
	failures        int
	attempts        int
	flaggedAttempts int
	flaggedSheds    int
	healthyInFlight float64
}

func (r simResult) errorRate() float64 {
	if r.requests == 0 {
		return 0
	}
	return float64(r.failures) / float64(r.requests)
}

func (r simResult) attemptsPerRequest() float64 {
	if r.requests == 0 {
		return 0
	}
	return float64(r.attempts) / float64(r.requests)
}

func (r simResult) flaggedShare() float64 {
	if r.attempts == 0 {
		return 0
	}
	return float64(r.flaggedAttempts) / float64(r.attempts)
}

func runPoolSim(t *testing.T, mode simScheduler, seed int64, lambda float64, seconds float64) simResult {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	const dt = 0.05
	const serviceTime = 10.0 // 模拟长对话请求
	const pendingTimeout = 5.0

	accounts := []*simAccount{
		{id: 1, upstreamCap: 5, configured: 5, window: 5},
		{id: 2, upstreamCap: 5, configured: 5, window: 5},
		{id: 3, upstreamCap: 1, configured: 5, window: 5, flagged: true},
		{id: 4, upstreamCap: 1, configured: 5, window: 5, flagged: true},
		{id: 5, upstreamCap: 1, configured: 5, window: 5, flagged: true},
	}
	params := DefaultAdaptiveConcurrencyParams()
	baseTime := time.Unix(1_700_000_000, 0)
	at := func(now float64) time.Time { return baseTime.Add(time.Duration(now * float64(time.Second))) }

	effective := func(a *simAccount) int {
		if mode == simAIMD {
			return EffectiveConcurrency(CwndSnapshot{Window: a.window}, a.configured)
		}
		return a.configured
	}
	pick := func(excluded map[int64]struct{}) *simAccount {
		var best *simAccount
		bestScore := math.Inf(1)
		bestHeadroom := math.Inf(-1)
		for _, a := range accounts {
			if _, skip := excluded[a.id]; skip {
				continue
			}
			if mode == simAIMD {
				headroom := effective(a) - a.inflight
				if headroom < 1 {
					continue
				}
				if float64(headroom) > bestHeadroom ||
					(float64(headroom) == bestHeadroom && best != nil && effective(a) > effective(best)) {
					best, bestHeadroom = a, float64(headroom)
				}
				continue
			}
			score := float64(a.inflight) * 100 / float64(a.configured)
			if score < bestScore {
				best, bestScore = a, score
			}
		}
		return best
	}

	res := simResult{}
	var pending []simPending

	dispatch := func(now float64) bool {
		excluded := make(map[int64]struct{}, len(accounts))
		sameAccountRetries := 0
		maxAttempts := 3 // 自适应窗口：单请求最多 3 次上游尝试（换号预算）
		if mode != simAIMD {
			maxAttempts = 44 // 现状：同账号重试 3 次 × 最多 11 个账号
		}
		for attempt := 0; attempt < maxAttempts; attempt++ {
			a := pick(excluded)
			if a == nil {
				return false
			}
			res.attempts++
			a.attempts++
			if a.flagged {
				res.flaggedAttempts++
			}
			if a.inflight >= a.upstreamCap {
				a.sheds++
				if a.flagged {
					res.flaggedSheds++
				}
				if mode == simAIMD {
					snap := NextCwnd(CwndSnapshot{Window: a.window, LastShedAt: a.lastShed, LastUpdateAt: a.lastUpdate}, a.configured, CwndOutcomeShed, at(now), params)
					a.window, a.lastShed, a.lastUpdate = snap.Window, snap.LastShedAt, snap.LastUpdateAt
					excluded[a.id] = struct{}{}
					continue
				}
				// 现状：同账号重试 3 次后再换号。
				if sameAccountRetries < 3 {
					sameAccountRetries++
					continue
				}
				sameAccountRetries = 0
				excluded[a.id] = struct{}{}
				continue
			}
			a.inflight++
			a.completions = append(a.completions, now+serviceTime)
			return true
		}
		return false
	}

	for step := 0; ; step++ {
		now := float64(step) * dt
		if now >= seconds {
			break
		}
		for _, a := range accounts {
			for len(a.completions) > 0 && a.completions[0] <= now {
				a.completions = a.completions[1:]
				a.inflight--
				if mode == simAIMD {
					snap := NextCwnd(CwndSnapshot{Window: a.window, LastShedAt: a.lastShed, LastUpdateAt: a.lastUpdate}, a.configured, CwndOutcomeSuccess, at(now), params)
					a.window, a.lastShed, a.lastUpdate = snap.Window, snap.LastShedAt, snap.LastUpdateAt
				}
			}
		}
		// 先处理排队中的请求。
		if len(pending) > 0 {
			remaining := pending[:0]
			for _, p := range pending {
				if dispatch(now) {
					continue
				}
				if now >= p.deadline {
					res.failures++
					continue
				}
				remaining = append(remaining, p)
			}
			pending = remaining
		}
		if rng.Float64() < lambda*dt {
			res.requests++
			if !dispatch(now) {
				if mode == simAIMD {
					pending = append(pending, simPending{deadline: now + pendingTimeout})
				} else {
					res.failures++
				}
			}
		}
	}
	// 未在仿真窗口内完成的排队请求计为失败。
	res.failures += len(pending)
	return res
}

func TestSmallPoolAIMDMitigatesOverloadedAccounts(t *testing.T) {
	const seed = 42
	const seconds = 600

	for _, lambda := range []float64{0.8, 1.0, 1.2} {
		lambda := lambda
		t.Run(fmt.Sprintf("lambda=%.1f", lambda), func(t *testing.T) {
			baseline := runPoolSim(t, simBaseline, seed, lambda, seconds)
			aimd := runPoolSim(t, simAIMD, seed, lambda, seconds)

			t.Logf("baseline: requests=%d failures=%d errorRate=%.3f attempts/req=%.2f flaggedShare=%.2f flaggedSheds=%d",
				baseline.requests, baseline.failures, baseline.errorRate(), baseline.attemptsPerRequest(), baseline.flaggedShare(), baseline.flaggedSheds)
			t.Logf("aimd:     requests=%d failures=%d errorRate=%.3f attempts/req=%.2f flaggedShare=%.2f flaggedSheds=%d",
				aimd.requests, aimd.failures, aimd.errorRate(), aimd.attemptsPerRequest(), aimd.flaggedShare(), aimd.flaggedSheds)

			require.Less(t, aimd.errorRate(), baseline.errorRate()*0.3, "自适应窗口应把用户侧错误率压到现状的三成以下")
			require.Less(t, aimd.attemptsPerRequest(), baseline.attemptsPerRequest()*0.5, "自适应窗口应把上游放大倍数压到现状的一半以下")
			require.Less(t, aimd.flaggedShare(), baseline.flaggedShare()*0.5, "坏账号的流量份额应显著低于现状")
			require.Less(t, aimd.flaggedSheds, baseline.flaggedSheds/10, "坏账号被反复打爆的次数应下降一个数量级")
		})
	}
}
