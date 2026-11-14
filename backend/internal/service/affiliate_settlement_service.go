package service

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// affiliateSettlementTimeout 单次结算的超时；批次上限见 affiliateSettlementBatchLimit。
	affiliateSettlementTimeout = 60 * time.Second

	// affiliateSettlementLeaderLockKey 保证多实例部署下只有一个实例推进结算游标。
	affiliateSettlementLeaderLockKey = "affiliate:usage-rebate:settle:leader"
	// affiliateSettlementLeaderLockTTL 必须大于 affiliateSettlementTimeout。
	affiliateSettlementLeaderLockTTL = 5 * time.Minute

	// affiliateSettlementBatchLimit 单次结算最多处理多少条用量，避免长事务。
	affiliateSettlementBatchLimit = 5000
)

// AffiliateSettlementService 周期性地把被邀请人的新增消费折算成邀请人的返利。
//
// 返利分两档：充值档在订单履约时立即计提（默认冻结 24 小时），消费档由这里增量结算。
// 结算游标与流水写入在同一个事务里完成，因此重复运行不会重复发放。
type AffiliateSettlementService struct {
	affiliateSvc *AffiliateService
	interval     time.Duration
	stopCh       chan struct{}
	stopOnce     sync.Once
	wg           sync.WaitGroup

	lockCache  LeaderLockCache
	db         *sql.DB
	instanceID string
}

func NewAffiliateSettlementService(affiliateSvc *AffiliateService, interval time.Duration) *AffiliateSettlementService {
	return &AffiliateSettlementService{
		affiliateSvc: affiliateSvc,
		interval:     interval,
		stopCh:       make(chan struct{}),
		instanceID:   uuid.NewString(),
	}
}

// SetLeaderLock 注入分布式锁；两者都为 nil 时不加锁（单实例或测试场景）。
func (s *AffiliateSettlementService) SetLeaderLock(lockCache LeaderLockCache, db *sql.DB) {
	if s == nil {
		return
	}
	s.lockCache = lockCache
	s.db = db
}

func (s *AffiliateSettlementService) Start() {
	if s == nil || s.affiliateSvc == nil || s.interval <= 0 {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		s.runOnce()
		for {
			select {
			case <-ticker.C:
				s.runOnce()
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *AffiliateSettlementService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
}

func (s *AffiliateSettlementService) runOnce() {
	lockCtx, lockCancel := context.WithTimeout(context.Background(), 2*time.Second)
	release, ok := tryAcquireSingletonLeaderLock(lockCtx, s.lockCache, s.db, affiliateSettlementLeaderLockKey, s.instanceID, affiliateSettlementLeaderLockTTL)
	lockCancel()
	if !ok {
		return
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), affiliateSettlementTimeout)
	defer cancel()

	result, err := s.affiliateSvc.SettleUsageRebate(ctx, affiliateSettlementBatchLimit)
	if err != nil {
		slog.Warn("[AffiliateSettlement] failed to settle usage rebate", "error", err)
		return
	}
	if result.Amount > 0 {
		slog.Info("[AffiliateSettlement] settled usage rebate",
			"amount", result.Amount,
			"pairs", result.Pairs,
			"lastUsageID", result.LastUsageID,
		)
	}
}
