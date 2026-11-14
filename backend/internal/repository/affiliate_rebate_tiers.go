package repository

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

const (
	// defaultUsageRebateBatchLimit 单次结算处理的用量行数上限。
	// 结算与游标推进在同一个事务里完成，批次过大只会拉长事务，不会提高吞吐。
	defaultUsageRebateBatchLimit = 5000

	usageRebateAction   = "accrue_usage"
	reverseRebateAction = "reverse"
)

// roundRebateAmount 与充值档保持一致：保留 8 位小数，避免浮点尾差在账本里累积。
func roundRebateAmount(v float64) float64 {
	return math.Round(v*1e8) / 1e8
}

// SettleUsageRebate 把游标之后新增的用量折算成邀请人的消费返利。
//
// 幂等性由「游标推进与流水写入同事务」保证：进程中途退出时整个事务回滚，
// 下次重跑从同一个游标继续，不会重复发放。
// 只结算绑定关系建立之后产生的用量（游标在迁移时初始化为当时的 MAX(usage_logs.id)）。
func (r *affiliateRepository) SettleUsageRebate(ctx context.Context, opts service.AffiliateUsageRebateOptions) (service.AffiliateUsageRebateSettlement, error) {
	var result service.AffiliateUsageRebateSettlement
	if opts.RatePercent <= 0 {
		return result, nil
	}
	batchLimit := opts.BatchLimit
	if batchLimit <= 0 {
		batchLimit = defaultUsageRebateBatchLimit
	}

	err := r.withTx(ctx, func(txCtx context.Context, txClient *dbent.Client) error {
		cursor, err := ensureUsageRebateCursorTx(txCtx, txClient)
		if err != nil {
			return err
		}

		upper, err := resolveUsageRebateUpperBound(txCtx, txClient, cursor, batchLimit)
		if err != nil {
			return err
		}
		if upper <= cursor {
			result.LastUsageID = cursor
			return nil
		}

		now := time.Now()
		totals := make(map[int64]float64)

		rows, err := txClient.QueryContext(txCtx, `
SELECT ul.user_id,
       ua.inviter_id,
       COALESCE(SUM(ul.actual_cost), 0)::double precision AS consumed,
       MAX(ua.created_at) AS bound_at,
       MAX(inv.aff_rebate_rate_percent)::double precision AS custom_rate
FROM usage_logs ul
JOIN user_affiliates ua ON ua.user_id = ul.user_id
JOIN user_affiliates inv ON inv.user_id = ua.inviter_id
WHERE ul.id > $1
  AND ul.id <= $2
  AND ul.actual_cost > 0
  AND ua.inviter_id IS NOT NULL
  AND ua.inviter_id <> ul.user_id
GROUP BY ul.user_id, ua.inviter_id`, cursor, upper)
		if err != nil {
			return fmt.Errorf("aggregate affiliate usage rebate: %w", err)
		}

		type usageRebatePair struct {
			inviteeID  int64
			inviterID  int64
			consumed   float64
			boundAt    time.Time
			customRate sql.NullFloat64
		}
		pairs := make([]usageRebatePair, 0, 32)
		for rows.Next() {
			var p usageRebatePair
			if err := rows.Scan(&p.inviteeID, &p.inviterID, &p.consumed, &p.boundAt, &p.customRate); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan affiliate usage rebate: %w", err)
			}
			pairs = append(pairs, p)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close affiliate usage rebate rows: %w", err)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate affiliate usage rebate: %w", err)
		}

		for _, p := range pairs {
			// 返利有效期：只结算绑定后有效期内的消费。
			if opts.DurationDays > 0 && now.After(p.boundAt.AddDate(0, 0, opts.DurationDays)) {
				continue
			}
			rate := opts.RatePercent
			if p.customRate.Valid {
				rate = p.customRate.Float64
			}
			amount := roundRebateAmount(p.consumed * rate / 100)
			if amount <= 0 {
				continue
			}
			if opts.PerInviteeCap > 0 {
				existing, err := accruedRebateFromInviteeTx(txCtx, txClient, p.inviterID, p.inviteeID)
				if err != nil {
					return err
				}
				remaining := opts.PerInviteeCap - existing
				if remaining <= 0 {
					continue
				}
				if amount > remaining {
					amount = roundRebateAmount(remaining)
				}
			}
			if amount <= 0 {
				continue
			}
			if _, err := txClient.ExecContext(txCtx, `
INSERT INTO user_affiliate_ledger (user_id, action, amount, source_user_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, NOW(), NOW())`,
				p.inviterID, usageRebateAction, amount, p.inviteeID); err != nil {
				return fmt.Errorf("insert affiliate usage rebate ledger: %w", err)
			}
			totals[p.inviterID] += amount
			result.Amount += amount
			result.Pairs++
		}

		for inviterID, amount := range totals {
			res, err := txClient.ExecContext(txCtx, `
UPDATE user_affiliates
SET aff_quota = aff_quota + $1,
    aff_history_quota = aff_history_quota + $1,
    updated_at = NOW()
WHERE user_id = $2`, amount, inviterID)
			if err != nil {
				return fmt.Errorf("credit affiliate usage rebate: %w", err)
			}
			if affected, _ := res.RowsAffected(); affected == 0 {
				// 流水已写但没有可加额度的邀请人档案：回滚整批，避免账实不符。
				return fmt.Errorf("credit affiliate usage rebate: inviter %d not found", inviterID)
			}
		}

		if _, err := txClient.ExecContext(txCtx, `
UPDATE affiliate_usage_rebate_cursor
SET last_usage_id = $1, updated_at = NOW()
WHERE id = 1`, upper); err != nil {
			return fmt.Errorf("advance affiliate usage rebate cursor: %w", err)
		}
		result.Amount = roundRebateAmount(result.Amount)
		result.LastUsageID = upper
		return nil
	})
	if err != nil {
		return service.AffiliateUsageRebateSettlement{}, err
	}
	return result, nil
}

// ReverseRebateForOrder 冲销某笔订单产生的返利（退款终态调用）。
//
// 已转成余额的部分允许把 aff_quota 扣成负数：它表示邀请人欠平台的返利，
// 后续计提先抵扣它，转余额接口要求 aff_quota > 0 因此会自然被挡住。
// 返回本次冲销的返利金额；重复调用是幂等的。
func (r *affiliateRepository) ReverseRebateForOrder(ctx context.Context, sourceOrderID, inviteeUserID int64, ratio float64) (float64, error) {
	if sourceOrderID <= 0 {
		return 0, nil
	}
	var reversed float64
	err := r.withTx(ctx, func(txCtx context.Context, txClient *dbent.Client) error {
		rows, err := txClient.QueryContext(txCtx, `
WITH accrued AS (
    SELECT l.user_id,
           COALESCE(SUM(CASE WHEN l.action IN ('accrue', 'accrue_usage') THEN l.amount ELSE 0 END), 0)::double precision AS total,
           COALESCE(SUM(CASE WHEN l.action IN ('accrue', 'accrue_usage') AND l.frozen_until IS NOT NULL THEN l.amount ELSE 0 END), 0)::double precision AS frozen,
           COALESCE(SUM(CASE WHEN l.action = 'reverse' THEN -l.amount ELSE 0 END), 0)::double precision AS done
    FROM user_affiliate_ledger l
    WHERE l.source_order_id = $1
    GROUP BY l.user_id
)
SELECT user_id, total, frozen, done FROM accrued`, sourceOrderID)
		if err != nil {
			return fmt.Errorf("load affiliate rebate for order: %w", err)
		}
		type orderRebate struct {
			inviterID int64
			net       float64
			frozen    float64
		}
		var pending []orderRebate
		for rows.Next() {
			var inviterID int64
			var total, frozen, done float64
			if err := rows.Scan(&inviterID, &total, &frozen, &done); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan affiliate rebate for order: %w", err)
			}
			// 部分退款按比例冲销：net 是这笔订单仍未被冲销的返利，ratio 是本次退款占比。
			net := roundRebateAmount((total - done) * ratio)
			if net <= 0 {
				continue
			}
			pending = append(pending, orderRebate{inviterID: inviterID, net: net, frozen: frozen})
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close affiliate rebate rows: %w", err)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate affiliate rebate rows: %w", err)
		}
		if len(pending) == 0 {
			return nil
		}

		for _, item := range pending {
			frozenPart := math.Min(item.frozen, item.net)
			if _, err := txClient.ExecContext(txCtx, `
INSERT INTO user_affiliate_ledger (user_id, action, amount, source_user_id, source_order_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, NOW(), NOW())`,
				item.inviterID, reverseRebateAction, -item.net, inviteeUserID, sourceOrderID); err != nil {
				return fmt.Errorf("insert affiliate rebate reverse ledger: %w", err)
			}
			// 清掉冻结标记：这些返利已被反向流水抵消，不能再在冻结期满后变成可用额度。
			if _, err := txClient.ExecContext(txCtx, `
UPDATE user_affiliate_ledger
SET frozen_until = NULL, updated_at = NOW()
WHERE source_order_id = $1
  AND action IN ('accrue', 'accrue_usage')
  AND frozen_until IS NOT NULL`, sourceOrderID); err != nil {
				return fmt.Errorf("clear frozen affiliate rebate for order: %w", err)
			}
			res, err := txClient.ExecContext(txCtx, `
UPDATE user_affiliates
SET aff_quota = aff_quota - $1,
    aff_frozen_quota = GREATEST(aff_frozen_quota - $2, 0),
    aff_history_quota = GREATEST(aff_history_quota - $1, 0),
    updated_at = NOW()
WHERE user_id = $3`, item.net, frozenPart, item.inviterID)
			if err != nil {
				return fmt.Errorf("debit affiliate rebate after refund: %w", err)
			}
			if affected, _ := res.RowsAffected(); affected == 0 {
				return fmt.Errorf("debit affiliate rebate after refund: inviter %d not found", item.inviterID)
			}
			reversed += item.net
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return roundRebateAmount(reversed), nil
}

// ensureUsageRebateCursorTx 读取结算游标；表里没有记录时按当前最大用量初始化，
// 保证任何情况下都不会把历史消费一次性折算成返利。
func ensureUsageRebateCursorTx(ctx context.Context, client *dbent.Client) (int64, error) {
	rows, err := client.QueryContext(ctx, `SELECT last_usage_id FROM affiliate_usage_rebate_cursor WHERE id = 1`)
	if err != nil {
		return 0, fmt.Errorf("read affiliate usage rebate cursor: %w", err)
	}
	if rows.Next() {
		var cursor int64
		if err := rows.Scan(&cursor); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan affiliate usage rebate cursor: %w", err)
		}
		if err := rows.Close(); err != nil {
			return 0, err
		}
		return cursor, nil
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}

	if _, err := client.ExecContext(ctx, `
INSERT INTO affiliate_usage_rebate_cursor (id, last_usage_id, updated_at)
SELECT 1, COALESCE((SELECT MAX(id) FROM usage_logs), 0), NOW()
ON CONFLICT (id) DO NOTHING`); err != nil {
		return 0, fmt.Errorf("init affiliate usage rebate cursor: %w", err)
	}
	rows, err = client.QueryContext(ctx, `SELECT last_usage_id FROM affiliate_usage_rebate_cursor WHERE id = 1`)
	if err != nil {
		return 0, fmt.Errorf("reload affiliate usage rebate cursor: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		var cursor int64
		if err := rows.Scan(&cursor); err != nil {
			return 0, err
		}
		return cursor, nil
	}
	return 0, nil
}

// resolveUsageRebateUpperBound 计算本次结算覆盖的用量上界（含）：
// 取第 batchLimit 条待结算用量的 id；不足一批时结算到当前最大 id。
func resolveUsageRebateUpperBound(ctx context.Context, client *dbent.Client, cursor int64, batchLimit int) (int64, error) {
	rows, err := client.QueryContext(ctx, `
SELECT id FROM usage_logs WHERE id > $1 ORDER BY id ASC LIMIT 1 OFFSET $2`, cursor, batchLimit-1)
	if err != nil {
		return 0, fmt.Errorf("resolve affiliate usage rebate upper bound: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}

	maxRows, err := client.QueryContext(ctx, `SELECT COALESCE(MAX(id), $1) FROM usage_logs`, cursor)
	if err != nil {
		return 0, fmt.Errorf("resolve affiliate usage rebate max id: %w", err)
	}
	defer func() { _ = maxRows.Close() }()
	if !maxRows.Next() {
		return cursor, nil
	}
	var maxID int64
	if err := maxRows.Scan(&maxID); err != nil {
		return 0, err
	}
	return maxID, nil
}

func accruedRebateFromInviteeTx(ctx context.Context, client *dbent.Client, inviterID, inviteeUserID int64) (float64, error) {
	rows, err := client.QueryContext(ctx,
		`SELECT COALESCE(SUM(amount), 0)::double precision FROM user_affiliate_ledger WHERE user_id = $1 AND source_user_id = $2 AND action IN ('accrue', 'accrue_usage', 'reverse')`,
		inviterID, inviteeUserID)
	if err != nil {
		return 0, fmt.Errorf("query accrued rebate from invitee: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var total float64
	if rows.Next() {
		if err := rows.Scan(&total); err != nil {
			return 0, err
		}
	}
	return total, nil
}
