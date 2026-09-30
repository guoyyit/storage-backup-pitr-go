package backup

import (
	"os"
	"sort"
	"time"
)

// ApplyRetention 按策略回收旧备份链，返回被删除的链 ID。
//
// 语义：RetainCount 保留最新 N 条链，RetainAge 保留该时长内创建的链，
// 两者同时设置时满足任一即保留；都为零时不回收任何链。
// 回收以整条链为单位，保留范围内的链的可恢复性不受影响。
// 与备份/恢复并发时结果确定：正在被恢复引用的链本轮跳过，
// 删除动作本身在链写锁内完成，不会出现删到一半的可见状态。
func (m *Manager) ApplyRetention(policy RetentionPolicy) ([]string, error) {
	if policy.RetainCount <= 0 && policy.RetainAge <= 0 {
		return nil, nil
	}
	m.mu.Lock()
	ids := make([]string, 0, len(m.chains))
	for id := range m.chains {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	// 链 ID 内嵌 UTC 时间戳，字典序即创建时间序。
	sort.Strings(ids)

	now := time.Now()
	var deleted []string
	for i, id := range ids {
		keepByCount := policy.RetainCount > 0 && len(ids)-i <= policy.RetainCount
		keepByAge := false
		if policy.RetainAge > 0 {
			if mf, err := readManifest(m.chainDir(id)); err == nil {
				keepByAge = now.Sub(mf.CreatedAt) < policy.RetainAge
			}
		}
		if keepByCount || keepByAge {
			continue
		}
		m.mu.Lock()
		h, ok := m.chains[id]
		m.mu.Unlock()
		if !ok {
			continue
		}
		// 恢复进行中（读锁被持有）的链本轮跳过，下轮再回收。
		if !h.mu.TryLock() {
			continue
		}
		if err := os.RemoveAll(m.chainDir(id)); err != nil {
			h.mu.Unlock()
			return deleted, err
		}
		m.mu.Lock()
		delete(m.chains, id)
		m.mu.Unlock()
		h.mu.Unlock()
		deleted = append(deleted, id)
	}
	if len(deleted) > 0 {
		m.refreshWALRetain()
	}
	return deleted, nil
}
