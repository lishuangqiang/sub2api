-- 邀请返利拆成两档：
--   1) 充值档：订单履约时按充值金额计提（比例 affiliate_rebate_rate，默认 10%），默认冻结 24 小时；
--   2) 消费档：按被邀请人实际消耗的额度计提（比例 affiliate_usage_rebate_rate，默认 15%）。
--
-- 消费档由定时任务增量结算，需要一个游标记录「已经结算到哪一条用量」。
--
-- 注意：本次升级同时调整了两项默认值（充值档 20% → 10%、冻结 0 → 24 小时），
-- 但这只影响从未写过这两项设置的部署。已有设置行的部署会保留原值，
-- 需要运营在后台显式改成 10% / 24 小时才会采用新算法。
CREATE TABLE IF NOT EXISTS affiliate_usage_rebate_cursor (
    id SMALLINT PRIMARY KEY,
    last_usage_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE affiliate_usage_rebate_cursor IS '邀请消费返利的结算游标（单行表），只结算游标之后新增的用量';

-- 初始化到当前最大 usage id。
-- 关键：不能从 0 开始，否则上线后第一次结算会把全部历史消费一次性折算成返利发放出去。
INSERT INTO affiliate_usage_rebate_cursor (id, last_usage_id, updated_at)
SELECT 1, COALESCE((SELECT MAX(id) FROM usage_logs), 0), NOW()
ON CONFLICT (id) DO NOTHING;
