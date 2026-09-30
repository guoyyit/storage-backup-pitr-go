package engine

import "sort"

// Memtable 保存已提交写操作应用后的最新内存状态。
type Memtable struct {
	data map[string][]byte
}

// NewMemtable 创建空 Memtable。
func NewMemtable() *Memtable {
	return &Memtable{data: make(map[string][]byte)}
}

// Get 返回 key 的最新值。
func (m *Memtable) Get(key string) ([]byte, bool) {
	v, ok := m.data[key]
	return v, ok
}

// Apply 将一条已提交记录应用到内存状态。
func (m *Memtable) Apply(rec *Record) {
	if rec.Op == OpDelete {
		delete(m.data, rec.Key)
		return
	}
	m.data[rec.Key] = append([]byte(nil), rec.Value...)
}

// Entries 返回当前全部键值（按 key 排序）的拷贝，用于一致性快照。
func (m *Memtable) Entries() []Record {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]Record, 0, len(keys))
	for _, k := range keys {
		out = append(out, Record{
			Op:    OpPut,
			Key:   k,
			Value: append([]byte(nil), m.data[k]...),
		})
	}
	return out
}

// Len 返回当前 key 数量。
func (m *Memtable) Len() int {
	return len(m.data)
}
