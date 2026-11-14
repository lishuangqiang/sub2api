//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// usageRebateSettlementRepoStub 只实现消费返利相关的两个方法，
// 其余接口方法由内嵌的 nil 接口兜底（被调用会 panic，正好暴露意外依赖）。
type usageRebateSettlementRepoStub struct {
	AffiliateRepository
	settleCalls int
	lastOpts    AffiliateUsageRebateOptions
	result      AffiliateUsageRebateSettlement
	err         error
	reverseArgs []int64
	reverseAmt  float64
	lastRatio   float64
}

func (s *usageRebateSettlementRepoStub) SettleUsageRebate(_ context.Context, opts AffiliateUsageRebateOptions) (AffiliateUsageRebateSettlement, error) {
	s.settleCalls++
	s.lastOpts = opts
	return s.result, s.err
}

func (s *usageRebateSettlementRepoStub) ReverseRebateForOrder(_ context.Context, sourceOrderID, inviteeUserID int64, ratio float64) (float64, error) {
	s.reverseArgs = []int64{sourceOrderID, inviteeUserID}
	s.lastRatio = ratio
	return s.reverseAmt, nil
}

// 返利总开关关闭时不得结算：避免运营关掉活动后定时任务还在发钱。
func TestSettleUsageRebateSkippedWhenAffiliateDisabled(t *testing.T) {
	t.Parallel()
	repo := &usageRebateSettlementRepoStub{result: AffiliateUsageRebateSettlement{Amount: 9}}
	settings := NewSettingService(&paymentFulfillmentSettingRepoStub{values: map[string]string{
		SettingKeyAffiliateEnabled:         "false",
		SettingKeyAffiliateUsageRebateRate: "15",
	}}, nil)
	svc := &AffiliateService{repo: repo, settingService: settings}

	got, err := svc.SettleUsageRebate(context.Background(), 100)
	require.NoError(t, err)
	require.Zero(t, got.Amount)
	require.Zero(t, repo.settleCalls, "关闭返利后不应触达仓储层")
}

// 消费档参数必须来自设置项：比例、有效期、单人上限都要透传给结算。
func TestSettleUsageRebatePassesConfiguredOptions(t *testing.T) {
	t.Parallel()
	repo := &usageRebateSettlementRepoStub{result: AffiliateUsageRebateSettlement{Amount: 3, Pairs: 1}}
	settings := NewSettingService(&paymentFulfillmentSettingRepoStub{values: map[string]string{
		SettingKeyAffiliateEnabled:             "true",
		SettingKeyAffiliateUsageRebateRate:     "15",
		SettingKeyAffiliateRebateDurationDays:  "365",
		SettingKeyAffiliateRebatePerInviteeCap: "50",
	}}, nil)
	svc := &AffiliateService{repo: repo, settingService: settings}

	got, err := svc.SettleUsageRebate(context.Background(), 100)
	require.NoError(t, err)
	require.Equal(t, 1, repo.settleCalls)
	require.InDelta(t, 15.0, repo.lastOpts.RatePercent, 1e-9)
	require.Equal(t, 365, repo.lastOpts.DurationDays)
	require.InDelta(t, 50.0, repo.lastOpts.PerInviteeCap, 1e-9)
	require.Equal(t, 100, repo.lastOpts.BatchLimit)
	require.InDelta(t, 3.0, got.Amount, 1e-9)
}

// 消费档比例未配置时回退到文档默认值 15%。
func TestSettleUsageRebateFallsBackToDefaultRate(t *testing.T) {
	t.Parallel()
	repo := &usageRebateSettlementRepoStub{}
	settings := NewSettingService(&paymentFulfillmentSettingRepoStub{values: map[string]string{
		SettingKeyAffiliateEnabled: "true",
	}}, nil)
	svc := &AffiliateService{repo: repo, settingService: settings}

	_, err := svc.SettleUsageRebate(context.Background(), 0)
	require.NoError(t, err)
	require.InDelta(t, AffiliateUsageRebateRateDefault, repo.lastOpts.RatePercent, 1e-9)
}

// 退款冲销必须把订单号与被邀请人一并传下去；订单号非法时是空操作。
func TestReverseInviteRebateForOrder(t *testing.T) {
	t.Parallel()
	repo := &usageRebateSettlementRepoStub{reverseAmt: 2}
	svc := &AffiliateService{repo: repo}

	got, err := svc.ReverseInviteRebateForOrder(context.Background(), 42, 7, 0.5)
	require.NoError(t, err)
	require.InDelta(t, 2.0, got, 1e-9)
	require.Equal(t, []int64{42, 7}, repo.reverseArgs)
	require.InDelta(t, 0.5, repo.lastRatio, 1e-9)

	repo.reverseArgs = nil
	_, err = svc.ReverseInviteRebateForOrder(context.Background(), 0, 7, 1)
	require.NoError(t, err)
	require.Nil(t, repo.reverseArgs, "非法订单号不应触达仓储层")
}

// 两档算法的默认值：充值 10%、冻结 24 小时、消费 15%。
func TestAffiliateRebateTierDefaults(t *testing.T) {
	t.Parallel()
	require.InDelta(t, 10.0, AffiliateRebateRateDefault, 1e-9)
	require.InDelta(t, 15.0, AffiliateUsageRebateRateDefault, 1e-9)
	require.Equal(t, 24, AffiliateRebateFreezeHoursDefault)
}
