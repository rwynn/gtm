package gtm

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
	"fmt"
	"strings"
)

type Op struct {
	Id bson.ObjectId
	Operation string
	Namespace string
	Data map[string]interface{}
	Timestamp bson.MongoTimestamp
}

type OpChan chan *Op

type OpLogEntry map[string]interface{}

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
	// only parse inserts, deletes, and updates	
	if this.IsInsert() || this.IsDelete() || this.IsUpdate() {
		var objectField OpLogEntry
		if this.IsUpdate() {
			objectField = entry["o2"].(OpLogEntry)
		} else {
			objectField = entry["o"].(OpLogEntry)
		}
		this.Id = objectField["_id"].(bson.ObjectId)
		this.Namespace = entry["ns"].(string)
		this.Timestamp = entry["ts"].(bson.MongoTimestamp)
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

func AfterTimestampFunc(ts int32, ordinal int32) string {
	return fmt.Sprintf(`function() { 
		return this.ts.t > %v 
		|| (this.ts.t == %v && this.ts.i > %v)
		}`, ts, ts, ordinal)
}

func GetOpLogQuery(collection *mgo.Collection, after bson.MongoTimestamp) *mgo.Query {
	tsFun := AfterTimestampFunc(ParseTimestamp(after))
	return collection.Find(bson.M{"$where": tsFun}).Sort("$natural")
}

func TailOps(session *mgo.Session, channel OpChan,
	errChan chan error, timeout string, after bson.MongoTimestamp) error {
	s := session.Copy()
	defer s.Close()
	collection := OpLogCollection(s)
	duration, err := time.ParseDuration(timeout)
	if err != nil {
		errChan <- err
		return err
	}
	iter := GetOpLogQuery(collection, after).Tail(duration)
	for {
		entry := make(OpLogEntry)
		for iter.Next(entry) {
			op := &Op{"", "", "", nil, bson.MongoTimestamp(0)}
			op.ParseLogEntry(entry)
			if op.Id != "" {
				after = op.Timestamp
				channel <- op
			}
		}
		if err = iter.Close(); err != nil {
			errChan <- err
			return err
		}
		if iter.Timeout() {
			continue
		}
		iter = GetOpLogQuery(collection, after).Tail(duration)
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

func TailAfter(session *mgo.Session, timestamp bson.MongoTimestamp) (OpChan, chan error) {
	inErr := make(chan error)
	outErr := make(chan error)
	inOp := make(OpChan)
	outOp := make(OpChan)
	go FetchDocuments(session, inOp, inErr, outOp, outErr)
	go TailOps(session, inOp, inErr, "5s", timestamp)
	return outOp, outErr
}

func Tail(session *mgo.Session) (OpChan, chan error) {
	return TailAfter(session, bson.MongoTimestamp(0))
}
