package backup

import "time"

// SegmentKind 区分备份链中的段类型。
type SegmentKind string

const (
	SegmentFull SegmentKind = "full"
	SegmentIncr SegmentKind = "incr"
)

// SegmentMeta 描述链上一个段（全量或增量）的元数据与校验信息。
type SegmentMeta struct {
	Kind     SegmentKind `json:"kind"`
	File     string      `json:"file"`     // 数据文件名（相对链目录）
	FromSeq  uint64      `json:"from_seq"` // 增量段起始（不含）；全量段为 0
	ToSeq    uint64      `json:"to_seq"`   // 段覆盖的最大 Seq（全量段为快照 Seq）
	Records  int         `json:"records"`
	SHA256   string      `json:"sha256"` // 数据文件内容摘要
	TimeUnix int64       `json:"time_unix"`
}

// Manifest 描述一条备份链：一个全量段 + 按序追加的增量段。
type Manifest struct {
	ChainID   string        `json:"chain_id"`
	CreatedAt time.Time     `json:"created_at"`
	Segments  []SegmentMeta `json:"segments"`
}

// RestoreOptions 恢复选项。
type RestoreOptions struct {
	// TargetDir 恢复写入的独立目标目录，不得与运行中引擎目录相同。
	TargetDir string
	// ChainID 指定恢复的备份链；为空时使用最新链。
	ChainID string
	// PointInTime 若非零，仅回放提交时间 <= 该时间点的记录。
	PointInTime time.Time
	// ToSeq 若非零，仅回放 Seq <= 该值的记录（与 PointInTime 同时生效时取更严者）。
	ToSeq uint64
}

// RetentionPolicy 保留策略：按数量或按时长保留备份链。
type RetentionPolicy struct {
	// RetainCount > 0 时仅保留最新 N 条链。
	RetainCount int
	// RetainAge > 0 时仅保留创建时间在该时长内的链。
	RetainAge time.Duration
}
