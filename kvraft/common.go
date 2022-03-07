package kvraft

type OpType string
type Err string

// ClientID 客户端ID
type ClientID int64

// OpSeriesID 每个客户端的自增操作ID
type OpSeriesID int64

const (
	GET    OpType = "Get"
	PUT    OpType = "Put"
	APPEND OpType = "Append"
)

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
	ErrFail        Err = "ErrFail"
)

type OpArgs struct {
	Key        string
	Value      string
	OpType     OpType
	ClientID   ClientID
	OpSeriesID OpSeriesID
}

type OpReply struct {
	Err   Err
	Value string
}
