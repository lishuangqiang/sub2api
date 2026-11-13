package service

// 二开：备用 / 降级分组。
//
// 上游只处理单账号故障切换（shouldFailoverUpstreamError 之后换同组账号）。
// 当整个分组额度耗尽，或者平台级故障导致组内账号一起不可用时，上游会直接
// 把失败返回给调用方。这里允许为每个分组配置一条备用分组链，按顺序接替。

import "sync"

// FallbackGroup 描述一个分组失效后可以接替它的分组链。
type FallbackGroup struct {
	GroupID          int64   `json:"group_id"`
	FallbackGroupIDs []int64 `json:"fallback_group_ids"`
	// 什么情况下才允许降级。两者都关表示不启用。
	OnQuotaExhausted bool `json:"on_quota_exhausted"`
	OnPlatformOutage bool `json:"on_platform_outage"`
}

// FallbackPlan 保存分组之间的降级关系，并回答「下一步该试哪一组」。
type FallbackPlan struct {
	mu    sync.RWMutex
	route map[int64]FallbackGroup
}

// NewFallbackPlan 从配置构建降级关系。重复的 GroupID 以最后一条为准。
func NewFallbackPlan(groups []FallbackGroup) *FallbackPlan {
	route := make(map[int64]FallbackGroup, len(groups))
	for _, group := range groups {
		route[group.GroupID] = group
	}
	return &FallbackPlan{route: route}
}

// Reload 用新配置整体替换降级关系，调用无需重建服务。
func (p *FallbackPlan) Reload(groups []FallbackGroup) {
	route := make(map[int64]FallbackGroup, len(groups))
	for _, group := range groups {
		route[group.GroupID] = group
	}
	p.mu.Lock()
	p.route = route
	p.mu.Unlock()
}

// Chain 返回 groupID 之后应当尝试的分组顺序。
//
// tried 由调用方持有并原地修改：已经尝试过的分组会被跳过，因此配置里写了
// 环（A→B→A）也不会导致无限重试。返回 nil 表示没有可用的备用分组。
func (p *FallbackPlan) Chain(groupID int64, tried map[int64]bool) []int64 {
	p.mu.RLock()
	group, ok := p.route[groupID]
	p.mu.RUnlock()
	if !ok {
		return nil
	}
	chain := make([]int64, 0, len(group.FallbackGroupIDs))
	for _, id := range group.FallbackGroupIDs {
		if id == groupID || tried[id] {
			continue
		}
		tried[id] = true
		chain = append(chain, id)
	}
	return chain
}

// ShouldFallback 判断当前失败是否满足该分组的降级条件。
func (p *FallbackPlan) ShouldFallback(groupID int64, quotaExhausted, platformOutage bool) bool {
	p.mu.RLock()
	group, ok := p.route[groupID]
	p.mu.RUnlock()
	if !ok {
		return false
	}
	return (group.OnQuotaExhausted && quotaExhausted) ||
		(group.OnPlatformOutage && platformOutage)
}
