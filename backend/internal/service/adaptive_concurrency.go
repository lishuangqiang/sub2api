package service

import (
	"context"
	"errors"
	"math"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// CwndOutcome 是一次上游尝试的结果分类，只有会影响账号容量判断的类别才进入窗口计算。
type CwndOutcome int

const (
	// CwndOutcomeNeutral 表示与账号容量无关的结果（4xx、鉴权失败、客户端取消、模型不存在等）。
	CwndOutcomeNeutral CwndOutcome = iota
	// CwndOutcomeSuccess 表示上游成功完成。
	CwndOutcomeSuccess
	// CwndOutcomeShed 表示上游容量型拒绝（503 overloaded / capacity shed）。
	CwndOutcomeShed
	// CwndOutcomeTransient 表示与容量相关的瞬时故障（5xx、超时、传输错误）。
	CwndOutcomeTransient
)

// AdaptiveConcurrencyParams 控制账号级自适应并发窗口（AIMD）的收敛速度。
type AdaptiveConcurrencyParams struct {
	// Alpha 是成功时的加性增长系数：w += Alpha / max(w, 1)。
	Alpha float64
	// Beta 是容量型拒绝时的乘性下降系数：w = max(MinWindow, w*Beta)。
	Beta float64
	// Gamma 是其他瞬时故障时的乘性下降系数。
	Gamma float64
	// Holddown 表示一次容量型拒绝后暂停增长的时长。
	Holddown time.Duration
	// IdleReset 表示空闲多久后开始向配置上限回归。
	IdleReset time.Duration
	// IdleStep 是每个 IdleReset 周期恢复的窗口值。
	IdleStep float64
	// MinWindow 是窗口下限，保证账号永远保留探测容量。
	MinWindow float64
	// Enabled 是总开关；关闭时调度行为与未启用前完全一致。
	Enabled bool
}

// DefaultAdaptiveConcurrencyParams 返回内置默认参数。
func DefaultAdaptiveConcurrencyParams() AdaptiveConcurrencyParams {
	return AdaptiveConcurrencyParams{
		Alpha:     0.5,
		Beta:      0.5,
		Gamma:     0.7,
		Holddown:  30 * time.Second,
		IdleReset: 10 * time.Minute,
		IdleStep:  1,
		MinWindow: 1,
		Enabled:   true,
	}
}

func (p AdaptiveConcurrencyParams) normalized() AdaptiveConcurrencyParams {
	def := DefaultAdaptiveConcurrencyParams()
	if p.Alpha <= 0 || p.Alpha > 10 {
		p.Alpha = def.Alpha
	}
	if p.Beta <= 0 || p.Beta >= 1 {
		p.Beta = def.Beta
	}
	if p.Gamma <= 0 || p.Gamma >= 1 {
		p.Gamma = def.Gamma
	}
	if p.Holddown <= 0 {
		p.Holddown = def.Holddown
	}
	if p.IdleReset <= 0 {
		p.IdleReset = def.IdleReset
	}
	if p.IdleStep <= 0 {
		p.IdleStep = def.IdleStep
	}
	if p.MinWindow <= 0 {
		p.MinWindow = def.MinWindow
	}
	if p.IdleReset < p.Holddown {
		p.IdleReset = p.Holddown
	}
	return p
}

// CwndSnapshot 是一个账号的自适应窗口状态。
type CwndSnapshot struct {
	// Window 是当前窗口值；零值表示尚未建立，按配置并发处理。
	Window float64
	// LastShedAt 是最近一次容量型拒绝的时间，用于 Holddown 判断。
	LastShedAt time.Time
	// LastUpdateAt 是最近一次窗口更新的时间，用于空闲恢复。
	LastUpdateAt time.Time
}

// NextCwnd 根据一次上游结果推进窗口。纯函数，便于单测与离线仿真。
func NextCwnd(prev CwndSnapshot, configured int, outcome CwndOutcome, now time.Time, params AdaptiveConcurrencyParams) CwndSnapshot {
	params = params.normalized()
	maxWindow := float64(configured)
	if maxWindow < params.MinWindow {
		maxWindow = params.MinWindow
	}

	snap := prev
	if snap.Window <= 0 {
		snap.Window = maxWindow
	}
	if !snap.LastUpdateAt.IsZero() && now.After(snap.LastUpdateAt) {
		idle := now.Sub(snap.LastUpdateAt)
		if idle > params.IdleReset {
			steps := math.Floor(float64(idle) / float64(params.IdleReset))
			snap.Window = math.Min(maxWindow, snap.Window+params.IdleStep*steps)
		}
	}

	switch outcome {
	case CwndOutcomeSuccess:
		// 刚被上游容量拒绝过的账号在 Holddown 内不增长，避免"恢复一次就被洪峰打回"。
		if snap.LastShedAt.IsZero() || now.Sub(snap.LastShedAt) >= params.Holddown {
			snap.Window = math.Min(maxWindow, snap.Window+params.Alpha/math.Max(snap.Window, 1))
		}
	case CwndOutcomeShed:
		snap.Window = math.Max(params.MinWindow, snap.Window*params.Beta)
		snap.LastShedAt = now
	case CwndOutcomeTransient:
		snap.Window = math.Max(params.MinWindow, snap.Window*params.Gamma)
	}

	snap.Window = math.Min(maxWindow, snap.Window)
	snap.LastUpdateAt = now
	return snap
}

// EffectiveConcurrency 把窗口转换为调度用的并发上限，始终落在 [1, configured] 内。
func EffectiveConcurrency(snap CwndSnapshot, configured int) int {
	if configured <= 0 {
		configured = 1
	}
	if snap.Window <= 0 {
		return configured
	}
	eff := int(math.Floor(snap.Window))
	if eff < 1 {
		eff = 1
	}
	if eff > configured {
		eff = configured
	}
	return eff
}

// ClassifyCwndOutcome 把一次转发尝试的结果映射为窗口更新类别。
// 只有真正与账号容量相关的结果才参与窗口计算，避免把请求侧错误算到账号头上。
func ClassifyCwndOutcome(success bool, observedErr error) CwndOutcome {
	if success {
		return CwndOutcomeSuccess
	}
	if observedErr == nil {
		return CwndOutcomeNeutral
	}
	if errors.Is(observedErr, context.Canceled) || errors.Is(observedErr, context.DeadlineExceeded) {
		// 客户端取消或调用方超时不能说明账号容量。
		return CwndOutcomeNeutral
	}
	var failoverErr *UpstreamFailoverError
	if errors.As(observedErr, &failoverErr) && failoverErr != nil {
		if failoverErr.RequestScopedTransient || failoverErr.StatusCode == http.StatusServiceUnavailable {
			return CwndOutcomeShed
		}
		switch {
		case failoverErr.StatusCode >= http.StatusInternalServerError,
			failoverErr.StatusCode == http.StatusRequestTimeout,
			failoverErr.StatusCode == http.StatusGatewayTimeout:
			return CwndOutcomeTransient
		default:
			return CwndOutcomeNeutral
		}
	}
	return CwndOutcomeNeutral
}

// AdaptiveConcurrencyStore 是自适应窗口的持久化边界。
type AdaptiveConcurrencyStore interface {
	// GetCwndBatch 批量读取窗口快照；未建立的账号返回零值快照。
	GetCwndBatch(ctx context.Context, accountIDs []int64) (map[int64]CwndSnapshot, error)
	// ReportCwnd 上报一次结果，实现需要保证并发安全，允许异步。
	ReportCwnd(ctx context.Context, accountID int64, configuredConcurrency int, outcome CwndOutcome)
}

// AdaptiveConcurrencyService 负责把窗口状态接到调度与结果上报上。
type AdaptiveConcurrencyService struct {
	store  AdaptiveConcurrencyStore
	params AdaptiveConcurrencyParams
}

// NewAdaptiveConcurrencyService 创建自适应并发服务；store 为 nil 时始终按配置并发处理。
func NewAdaptiveConcurrencyService(store AdaptiveConcurrencyStore, params AdaptiveConcurrencyParams) *AdaptiveConcurrencyService {
	return &AdaptiveConcurrencyService{store: store, params: params.normalized()}
}

// Enabled 报告自适应窗口是否生效。
func (s *AdaptiveConcurrencyService) Enabled() bool {
	return s != nil && s.store != nil && s.params.Enabled
}

// Params 返回归一化后的参数（供测试与观测使用）。
func (s *AdaptiveConcurrencyService) Params() AdaptiveConcurrencyParams {
	if s == nil {
		return DefaultAdaptiveConcurrencyParams()
	}
	return s.params
}

// Windows 批量返回账号 ID → 调度并发上限。读取失败时返回 nil，调用方回退到配置并发。
func (s *AdaptiveConcurrencyService) Windows(ctx context.Context, accounts []Account) map[int64]int {
	if !s.Enabled() || len(accounts) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(accounts))
	configured := make(map[int64]int, len(accounts))
	for i := range accounts {
		// 并发配置为 0/负数的账号语义是「不限并发」，不参与自适应窗口。
		if accounts[i].ID <= 0 || accounts[i].Concurrency <= 0 {
			continue
		}
		ids = append(ids, accounts[i].ID)
		configured[accounts[i].ID] = accounts[i].Concurrency
	}
	if len(ids) == 0 {
		return nil
	}
	snapshots, err := s.store.GetCwndBatch(ctx, ids)
	if err != nil {
		return nil
	}
	windows := make(map[int64]int, len(ids))
	for _, id := range ids {
		windows[id] = EffectiveConcurrency(snapshots[id], configured[id])
	}
	return windows
}

// Report 上报一次尝试结果。configuredConcurrency 传账号的配置并发。
func (s *AdaptiveConcurrencyService) Report(ctx context.Context, accountID int64, configuredConcurrency int, success bool, observedErr error) {
	if !s.Enabled() || accountID <= 0 {
		return
	}
	outcome := ClassifyCwndOutcome(success, observedErr)
	if outcome == CwndOutcomeNeutral {
		return
	}
	s.store.ReportCwnd(ctx, accountID, configuredConcurrency, outcome)
}

// adaptiveWindows 为候选账号批量读取自适应并发窗口；未启用时返回 nil。
func (s *GatewayService) adaptiveWindows(ctx context.Context, accounts []Account) map[int64]int {
	if s == nil || s.adaptiveConcurrency == nil || !s.adaptiveConcurrency.Enabled() {
		return nil
	}
	return s.adaptiveConcurrency.Windows(ctx, accounts)
}

func adaptiveWindowFor(windows map[int64]int, accountID int64) (int, bool) {
	if len(windows) == 0 {
		return 0, false
	}
	window, ok := windows[accountID]
	if !ok || window <= 0 {
		return 0, false
	}
	return window, true
}

// slotConcurrency 返回抢槽与等待计划使用的并发上限：自适应窗口与配置并发的较小值。
func (s *GatewayService) slotConcurrency(account *Account, windows map[int64]int) int {
	if account == nil {
		return 0
	}
	configured := account.Concurrency
	if configured > 0 {
		if window, ok := adaptiveWindowFor(windows, account.ID); ok && window < configured {
			return window
		}
	}
	return configured
}

// loadDenominator 返回负载率的分母：自适应窗口与负载因子的较小值。
func (s *GatewayService) loadDenominator(account *Account, windows map[int64]int) int {
	if account == nil {
		return 1
	}
	base := account.EffectiveLoadFactor()
	if base <= 0 {
		base = 1
	}
	if window, ok := adaptiveWindowFor(windows, account.ID); ok && window < base {
		return window
	}
	return base
}

// filterByMaxHeadroom 取「可用余量」最大的账号集合；余量相同时优先容量更大的账号，
// 避免把同等余量的机会让给已经被上游降级的账号。
func filterByMaxHeadroom(accounts []accountWithLoad, windows map[int64]int) []accountWithLoad {
	if len(accounts) == 0 {
		return accounts
	}
	bestHeadroom := headroomFor(accounts[0], windows)
	bestCapacity := capacityFor(accounts[0], windows)
	for _, item := range accounts[1:] {
		h := headroomFor(item, windows)
		c := capacityFor(item, windows)
		switch {
		case h > bestHeadroom:
			bestHeadroom, bestCapacity = h, c
		case h == bestHeadroom && c > bestCapacity:
			bestCapacity = c
		}
	}
	result := make([]accountWithLoad, 0, len(accounts))
	for _, item := range accounts {
		if headroomFor(item, windows) == bestHeadroom && capacityFor(item, windows) == bestCapacity {
			result = append(result, item)
		}
	}
	return result
}

// adaptiveWindows 为候选账号批量读取自适应并发窗口；未启用时返回 nil。
func (s *OpenAIGatewayService) adaptiveWindows(ctx context.Context, accounts []Account) map[int64]int {
	if s == nil || s.adaptiveConcurrency == nil || !s.adaptiveConcurrency.Enabled() {
		return nil
	}
	return s.adaptiveConcurrency.Windows(ctx, accounts)
}

// adaptiveWindowsPtr 是 []*Account 版本，供高级调度路径使用。
func (s *OpenAIGatewayService) adaptiveWindowsPtr(ctx context.Context, accounts []*Account) map[int64]int {
	if s == nil || s.adaptiveConcurrency == nil || !s.adaptiveConcurrency.Enabled() {
		return nil
	}
	list := make([]Account, 0, len(accounts))
	for _, account := range accounts {
		if account != nil {
			list = append(list, *account)
		}
	}
	return s.adaptiveConcurrency.Windows(ctx, list)
}

// slotConcurrency 返回抢槽与等待计划使用的并发上限（并发配置为 0 表示不限）。
func (s *OpenAIGatewayService) slotConcurrency(account *Account, windows map[int64]int) int {
	if account == nil {
		return 0
	}
	configured := account.Concurrency
	if configured > 0 {
		if window, ok := adaptiveWindowFor(windows, account.ID); ok && window < configured {
			return window
		}
	}
	return configured
}

// loadDenominator 返回负载率的分母：自适应窗口与负载因子的较小值。
func (s *OpenAIGatewayService) loadDenominator(account *Account, windows map[int64]int) int {
	if account == nil {
		return 1
	}
	base := account.EffectiveLoadFactor()
	if base <= 0 {
		base = 1
	}
	if window, ok := adaptiveWindowFor(windows, account.ID); ok && window < base {
		return window
	}
	return base
}

// headroomFor 计算账号的可用余量：容量（负载因子/窗口）减去在途与排队。
func headroomFor(item accountWithLoad, windows map[int64]int) int {
	if item.account == nil {
		return 0
	}
	capacity := item.account.EffectiveLoadFactor()
	if capacity <= 0 {
		capacity = 1
	}
	if window, ok := adaptiveWindowFor(windows, item.account.ID); ok && window < capacity {
		capacity = window
	}
	if item.loadInfo == nil {
		return capacity
	}
	return capacity - item.loadInfo.CurrentConcurrency - item.loadInfo.WaitingCount
}

// capacityFor 返回账号在本次调度中使用的容量（负载因子/窗口的较小值）。
func capacityFor(item accountWithLoad, windows map[int64]int) int {
	if item.account == nil {
		return 0
	}
	capacity := item.account.EffectiveLoadFactor()
	if capacity <= 0 {
		capacity = 1
	}
	if window, ok := adaptiveWindowFor(windows, item.account.ID); ok && window < capacity {
		capacity = window
	}
	return capacity
}

// AdaptiveConcurrencyParamsFromConfig 把配置结构转换为自适应窗口参数。
func AdaptiveConcurrencyParamsFromConfig(cfg config.GatewayAdaptiveConcurrencyConfig) AdaptiveConcurrencyParams {
	params := DefaultAdaptiveConcurrencyParams()
	params.Enabled = cfg.Enabled
	if cfg.Alpha > 0 {
		params.Alpha = cfg.Alpha
	}
	if cfg.Beta > 0 && cfg.Beta < 1 {
		params.Beta = cfg.Beta
	}
	if cfg.Gamma > 0 && cfg.Gamma < 1 {
		params.Gamma = cfg.Gamma
	}
	if cfg.Holddown > 0 {
		params.Holddown = cfg.Holddown
	}
	if cfg.IdleReset > 0 {
		params.IdleReset = cfg.IdleReset
	}
	if cfg.IdleStep > 0 {
		params.IdleStep = cfg.IdleStep
	}
	if cfg.MinWindow > 0 {
		params.MinWindow = cfg.MinWindow
	}
	return params
}
