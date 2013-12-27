package gtm

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
	"strings"
)

type Options struct {
	After TimestampGenerator
	Filter OpFilter
}

type Op struct {
	Id interface{}
	Operation string
	Namespace string
	Data map[string]interface{}
	Timestamp bson.MongoTimestamp
}

type OpLog struct {
	Timestamp bson.MongoTimestamp "ts"
	HistoryID int64 "h"
	MongoVersion int "v"
	Operation string "op"
	Namespace string "ns"
	Object bson.M "o"
	QueryObject bson.M "o2"
}

type OpChan chan *Op

type OpLogEntry map[string]interface{}

type OpFilter func (*Op) bool

type TimestampGenerator func(*mgo.Session) bson.MongoTimestamp

func ChainOpFilters(filters ...OpFilter) OpFilter {
	return func(op *Op) bool {
		for _, filter := range filters {
			if filter(op) == false {
				return false
			}
		}
		return true
	}
}

func (this *Op) IsInsert() bool {
	return this.Operation == "i"
}

func (this *Op) IsUpdate() bool {
	return this.Operation == "u"
}

func (this *Op) IsDelete() bool {
	return this.Operation == "d"
}

func (this *Op) ParseNamespace() []string {
	return strings.SplitN(this.Namespace, ".", 2)
}

func (this *Op) GetDatabase() string {
	return this.ParseNamespace()[0]
}

func (this *Op) GetCollection() string {
	return this.ParseNamespace()[1]
}

func (this *Op) FetchData(session *mgo.Session) error {
	if this.IsDelete() {
		return nil
	}
	collection := session.DB(this.GetDatabase()).C(this.GetCollection())
	doc := make(map[string]interface{})
	err := collection.FindId(this.Id).One(doc)
	if err == nil {
		this.Data = doc
		return nil
	} else {
		return err
	}
}

func (this *Op) ParseLogEntry(entry OpLogEntry) {
	this.Operation = entry["op"].(string)
	this.Timestamp = entry["ts"].(bson.MongoTimestamp)
	// only parse inserts, deletes, and updates	
	if this.IsInsert() || this.IsDelete() || this.IsUpdate() {
		var objectField OpLogEntry
		if this.IsUpdate() {
			objectField = entry["o2"].(OpLogEntry)
		} else {
			objectField = entry["o"].(OpLogEntry)
		}
		this.Id = objectField["_id"]
		this.Namespace = entry["ns"].(string)
	}
}

func OpLogCollection(session *mgo.Session) *mgo.Collection {
	collection := session.DB("local").C("oplog.rs")
	return collection
}

func ParseTimestamp(timestamp bson.MongoTimestamp) (int32, int32) {
	ordinal := (timestamp << 32) >> 32
	ts := (timestamp >> 32)
	return int32(ts), int32(ordinal)
}

func LastOpTimestamp(session *mgo.Session) bson.MongoTimestamp {
	var opLog OpLog
	collection := OpLogCollection(session)
	collection.Find(nil).Sort("-$natural").One(&opLog)
	return opLog.Timestamp
}

func GetOpLogQuery(session *mgo.Session, after bson.MongoTimestamp)  *mgo.Query {
	query := bson.M{"ts": bson.M{"$gt": after}}
	collection := OpLogCollection(session)
	return collection.Find(query).LogReplay().Sort("$natural")
}

func TailOps(session *mgo.Session, channel OpChan,
	errChan chan error, timeout string, options *Options) error {
	s := session.Copy()
	defer s.Close()
	duration, err := time.ParseDuration(timeout)
	if err != nil {
		errChan <- err
		return err
	}
	if options.After == nil {
		options.After = LastOpTimestamp
	}
	currTimestamp := options.After(s)
	iter := GetOpLogQuery(s, currTimestamp).Tail(duration)
	for {
		entry := make(OpLogEntry)
		for iter.Next(entry) {
			op := &Op{"", "", "", nil, bson.MongoTimestamp(0)}
			op.ParseLogEntry(entry)
			if op.Id != "" {
				if options.Filter == nil || options.Filter(op) {
					channel <- op
				}
			}
			currTimestamp = op.Timestamp
		}
		if err = iter.Close(); err != nil {
			errChan <- err
			return err
		}
		if iter.Timeout() {
			continue
		}
		iter = GetOpLogQuery(s, currTimestamp).Tail(duration)
	}
	return nil
}

func FetchDocuments(session *mgo.Session, inOp OpChan, inErr chan error,
	outOp OpChan, outErr chan error) error {
	s := session.Copy()
	defer s.Close()
	for {
		select {
		case err:= <-inErr:
			outErr <- err
		case op:= <-inOp:
			fetchErr := op.FetchData(s)
			if fetchErr == nil || fetchErr == mgo.ErrNotFound {
				outOp <- op
			} else {
				outErr <- fetchErr
			}
		}
	}
	return nil
}

func Tail(session *mgo.Session, options *Options) (OpChan, chan error) {
	inErr := make(chan error, 20)
	outErr := make(chan error, 20)
	inOp := make(OpChan, 20)
	outOp := make(OpChan, 20)
	go FetchDocuments(session, inOp, inErr, outOp, outErr)
	go TailOps(session, inOp, inErr, "100s", options)
	return outOp, outErr
}

