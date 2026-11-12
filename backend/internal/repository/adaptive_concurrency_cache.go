package repository

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const (
	adaptiveCwndKeyPrefix      = "acct:cwnd:"
	adaptiveCwndTTL            = time.Hour
	adaptiveCwndReportQueue    = 4096
	adaptiveCwndReportBatch    = 64
	adaptiveCwndReportInterval = 50 * time.Millisecond
	adaptiveCwndReportTimeout  = 2 * time.Second
)

// adaptiveCwndReportScript 是单 key 的原子窗口更新。
// KEYS[1] = acct:cwnd:{account_id}
// ARGV: outcome(1 成功 / -1 容量拒绝 / -2 瞬时故障), max, alpha, beta, gamma,
// holddown 秒, idleReset 秒, idleStep, minWindow, now 秒, ttl 秒
const adaptiveCwndReportScriptSource = `
local v = redis.call('HMGET', KEYS[1], 'w', 's', 'l')
local now = tonumber(ARGV[10])
local maxWindow = tonumber(ARGV[2])
local minWindow = tonumber(ARGV[9])
local w = tonumber(v[1]) or maxWindow
local s = tonumber(v[2]) or 0
local l = tonumber(v[3]) or now

local idleReset = tonumber(ARGV[7])
if l > 0 and now - l > idleReset then
  local steps = math.floor((now - l) / idleReset)
  w = math.min(maxWindow, w + tonumber(ARGV[8]) * steps)
end

local outcome = tonumber(ARGV[1])
if outcome == 1 then
  if s == 0 or now - s >= tonumber(ARGV[6]) then
    w = math.min(maxWindow, w + tonumber(ARGV[3]) / math.max(w, 1))
  end
elseif outcome == -1 then
  w = math.max(minWindow, w * tonumber(ARGV[4]))
  s = now
elseif outcome == -2 then
  w = math.max(minWindow, w * tonumber(ARGV[5]))
end

if w > maxWindow then w = maxWindow end
if w < minWindow then w = minWindow end

redis.call('HSET', KEYS[1], 'w', w, 's', s, 'l', now)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[11]))
return w
`

type adaptiveCwndReport struct {
	accountID  int64
	configured int
	outcome    service.CwndOutcome
}

type adaptiveConcurrencyCache struct {
	rdb       *redis.Client
	params    service.AdaptiveConcurrencyParams
	reportCh  chan adaptiveCwndReport
	startOnce sync.Once
}

// NewAdaptiveConcurrencyCache 创建账号级自适应并发窗口缓存。
func NewAdaptiveConcurrencyCache(rdb *redis.Client, params service.AdaptiveConcurrencyParams) service.AdaptiveConcurrencyStore {
	return &adaptiveConcurrencyCache{
		rdb:      rdb,
		params:   params,
		reportCh: make(chan adaptiveCwndReport, adaptiveCwndReportQueue),
	}
}

func adaptiveCwndKey(accountID int64) string {
	return adaptiveCwndKeyPrefix + strconv.FormatInt(accountID, 10)
}

// GetCwndBatch 使用一次 pipeline 批量读取窗口状态。
func (c *adaptiveConcurrencyCache) GetCwndBatch(ctx context.Context, accountIDs []int64) (map[int64]service.CwndSnapshot, error) {
	result := make(map[int64]service.CwndSnapshot, len(accountIDs))
	if c == nil || c.rdb == nil || len(accountIDs) == 0 {
		return result, nil
	}
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, 0, len(accountIDs))
	ids := make([]int64, 0, len(accountIDs))
	for _, id := range accountIDs {
		if id <= 0 {
			continue
		}
		cmds = append(cmds, pipe.HMGet(ctx, adaptiveCwndKey(id), "w", "s", "l"))
		ids = append(ids, id)
	}
	if len(cmds) == 0 {
		return result, nil
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	for i, cmd := range cmds {
		values, err := cmd.Result()
		if err != nil || len(values) == 0 {
			continue
		}
		var snap service.CwndSnapshot
		if window, ok := parseAdaptiveCwndFloat(values[0]); ok {
			snap.Window = window
		}
		if shedAt, ok := parseAdaptiveCwndFloat(values[1]); ok && shedAt > 0 {
			snap.LastShedAt = time.Unix(int64(shedAt), 0)
		}
		if updatedAt, ok := parseAdaptiveCwndFloat(values[2]); ok && updatedAt > 0 {
			snap.LastUpdateAt = time.Unix(int64(updatedAt), 0)
		}
		result[ids[i]] = snap
	}
	return result, nil
}

func parseAdaptiveCwndFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case string:
		if v == "" {
			return 0, false
		}
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	case []byte:
		parsed, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	case float64:
		return v, true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

// ReportCwnd 异步上报一次结果；队列满时丢弃样本，只影响收敛速度，不影响正确性。
func (c *adaptiveConcurrencyCache) ReportCwnd(_ context.Context, accountID int64, configuredConcurrency int, outcome service.CwndOutcome) {
	if c == nil || c.rdb == nil || accountID <= 0 {
		return
	}
	c.startOnce.Do(func() {
		go c.runReporter()
	})
	select {
	case c.reportCh <- adaptiveCwndReport{accountID: accountID, configured: configuredConcurrency, outcome: outcome}:
	default:
	}
}

func (c *adaptiveConcurrencyCache) runReporter() {
	ticker := time.NewTicker(adaptiveCwndReportInterval)
	defer ticker.Stop()

	batch := make([]adaptiveCwndReport, 0, adaptiveCwndReportBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), adaptiveCwndReportTimeout)
		pipe := c.rdb.Pipeline()
		for _, item := range batch {
			c.queueScript(pipe, ctx, item)
		}
		_, _ = pipe.Exec(ctx)
		cancel()
		batch = batch[:0]
	}

	for {
		select {
		case item := <-c.reportCh:
			batch = append(batch, item)
			if len(batch) >= adaptiveCwndReportBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (c *adaptiveConcurrencyCache) queueScript(pipe redis.Pipeliner, ctx context.Context, item adaptiveCwndReport) {
	outcome := int64(1)
	switch item.outcome {
	case service.CwndOutcomeShed:
		outcome = -1
	case service.CwndOutcomeTransient:
		outcome = -2
	}
	configured := item.configured
	if configured <= 0 {
		configured = 1
	}
	now := time.Now().Unix()
	_ = pipe.Eval(ctx, adaptiveCwndReportScriptSource, []string{adaptiveCwndKey(item.accountID)},
		outcome,
		configured,
		c.params.Alpha,
		c.params.Beta,
		c.params.Gamma,
		int64(c.params.Holddown.Seconds()),
		int64(c.params.IdleReset.Seconds()),
		c.params.IdleStep,
		c.params.MinWindow,
		now,
		int64(adaptiveCwndTTL.Seconds()),
	).Err()
}
