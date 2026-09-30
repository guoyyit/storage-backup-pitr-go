package engine

// Op 表示一条已提交写操作的类型。
type Op byte

const (
	OpPut Op = iota + 1
	OpDelete
)

// Record 是一条已提交的写操作记录。
// Seq 单调递增；Time 为提交时间（UnixNano），保证与 Seq 同序，
// 因此任意历史提交时间点都可以唯一映射到一个 Seq 边界。
type Record struct {
	Seq   uint64
	Time  int64
	Op    Op
	Key   string
	Value []byte
}
