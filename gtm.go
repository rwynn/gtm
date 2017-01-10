package gtm

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"strings"
	"time"
)

var seekChan = make(chan bson.MongoTimestamp, 1)
var pauseChan = make(chan bool, 1)
var resumeChan = make(chan bool, 1)
var paused bool = false

type Options struct {
	After               TimestampGenerator
	Filter              OpFilter
	OpLogDatabaseName   *string
	OpLogCollectionName *string
	CursorTimeout       *string
	ChannelSize         int
}

type Op struct {
	Id        interface{}            `json:"_id"`
	Operation string                 `json:"operation"`
	Namespace string                 `json:"namespace"`
	Data      map[string]interface{} `json:"data"`
	Timestamp bson.MongoTimestamp    `json:"timestamp"`
}

type OpLog struct {
	Timestamp    bson.MongoTimestamp "ts"
	HistoryID    int64               "h"
	MongoVersion int                 "v"
	Operation    string              "op"
	Namespace    string              "ns"
	Object       bson.M              "o"
	QueryObject  bson.M              "o2"
}

type OpChan chan *Op

type OpLogEntry map[string]interface{}

type OpFilter func(*Op) bool

type TimestampGenerator func(*mgo.Session, *Options) bson.MongoTimestamp

func Since(ts bson.MongoTimestamp) {
	seekChan <- ts
}

func Pause() {
	if !paused {
		paused = true
		pauseChan <- true
	}
}

func Resume() {
	if paused {
		paused = false
		resumeChan <- true
	}
}

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

func (this *Op) IsDrop() bool {
	if _, drop := this.IsDropDatabase(); drop {
		return true
	}
	if _, drop := this.IsDropCollection(); drop {
		return true
	}
	return false
}

func (this *Op) IsDropCollection() (string, bool) {
	if this.IsCommand() {
		if this.Data != nil {
			if val, ok := this.Data["drop"]; ok {
				return val.(string), true
			}
		}
	}
	return "", false
}

func (this *Op) IsDropDatabase() (string, bool) {
	if this.IsCommand() {
		if this.Data != nil {
			if _, ok := this.Data["dropDatabase"]; ok {
				return this.GetDatabase(), true
			}
		}
	}
	return "", false
}

func (this *Op) IsCommand() bool {
	return this.Operation == "c"
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
	if _, drop := this.IsDropDatabase(); drop {
		return ""
	} else if col, drop := this.IsDropCollection(); drop {
		return col
	} else {
		return this.ParseNamespace()[1]
	}
}

func (this *Op) FetchData(session *mgo.Session) error {
	if this.IsDelete() || this.IsCommand() {
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

func (this *Op) ParseLogEntry(entry OpLogEntry) (include bool) {
	this.Operation = entry["op"].(string)
	this.Timestamp = entry["ts"].(bson.MongoTimestamp)
	this.Namespace = entry["ns"].(string)
	if this.IsInsert() || this.IsDelete() || this.IsUpdate() {
		var objectField OpLogEntry
		if this.IsUpdate() {
			objectField = entry["o2"].(OpLogEntry)
		} else {
			objectField = entry["o"].(OpLogEntry)
		}
		this.Id = objectField["_id"]
		include = true
	} else if this.IsCommand() {
		this.Data = entry["o"].(OpLogEntry)
		include = this.IsDrop()
	} else {
		include = false
	}
	return
}

func OpLogCollectionName(session *mgo.Session, options *Options) string {
	localDB := session.DB(*options.OpLogDatabaseName)
	col_names, err := localDB.CollectionNames()
	if err == nil {
		var col_name *string = nil
		for _, name := range col_names {
			if strings.HasPrefix(name, "oplog.") {
				col_name = &name
				break
			}
		}
		if col_name == nil {
			msg := fmt.Sprintf(`
				Unable to find oplog collection 
				in database %v`, *options.OpLogDatabaseName)
			panic(msg)
		} else {
			return *col_name
		}
	} else {
		msg := fmt.Sprintf(`Unable to get collection names 
				for database %v`, *options.OpLogDatabaseName)
		panic(msg)
	}
}

func OpLogCollection(session *mgo.Session, options *Options) *mgo.Collection {
	localDB := session.DB(*options.OpLogDatabaseName)
	return localDB.C(*options.OpLogCollectionName)
}

func ParseTimestamp(timestamp bson.MongoTimestamp) (int32, int32) {
	ordinal := (timestamp << 32) >> 32
	ts := (timestamp >> 32)
	return int32(ts), int32(ordinal)
}

func LastOpTimestamp(session *mgo.Session, options *Options) bson.MongoTimestamp {
	var opLog OpLog
	collection := OpLogCollection(session, options)
	collection.Find(nil).Sort("-$natural").One(&opLog)
	return opLog.Timestamp
}

func GetOpLogQuery(session *mgo.Session, after bson.MongoTimestamp, options *Options) *mgo.Query {
	query := bson.M{"ts": bson.M{"$gt": after}}
	collection := OpLogCollection(session, options)
	return collection.Find(query).LogReplay().Sort("$natural")
}

func FillEmptyOptions(session *mgo.Session, options *Options) {
	if options.After == nil {
		options.After = LastOpTimestamp
	}
	if options.OpLogDatabaseName == nil {
		defaultOpLogDatabaseName := "local"
		options.OpLogDatabaseName = &defaultOpLogDatabaseName
	}
	if options.OpLogCollectionName == nil {
		defaultOpLogCollectionName := OpLogCollectionName(session, options)
		options.OpLogCollectionName = &defaultOpLogCollectionName
	}
	if options.CursorTimeout == nil {
		defaultCursorTimeout := "100s"
		options.CursorTimeout = &defaultCursorTimeout
	}
}

func TailOps(session *mgo.Session, channel OpChan, errChan chan error, options *Options) error {
	s := session.Copy()
	defer s.Close()
	FillEmptyOptions(session, options)
	duration, err := time.ParseDuration(*options.CursorTimeout)
	if err != nil {
		panic("Invalid value for CursorTimeout")
	}
	currTimestamp := options.After(s, options)
	iter := GetOpLogQuery(s, currTimestamp, options).Tail(duration)
	for {
		entry := make(OpLogEntry)
	Seek:
		for iter.Next(entry) {
			op := &Op{"", "", "", nil, bson.MongoTimestamp(0)}
			if op.ParseLogEntry(entry) {
				if options.Filter == nil || options.Filter(op) {
					channel <- op
				}
			}
			select {
			case ts := <-seekChan:
				currTimestamp = ts
				break Seek
			case <-pauseChan:
				<-resumeChan
				select {
				case ts := <-seekChan:
					currTimestamp = ts
					break Seek

				default:
					currTimestamp = op.Timestamp
				}
			default:
				currTimestamp = op.Timestamp
			}
		}
		if err = iter.Close(); err != nil {
			errChan <- err
			return err
		}
		if iter.Timeout() {
			select {
			case ts := <-seekChan:
				currTimestamp = ts
			case <-pauseChan:
				<-resumeChan
				select {
				case ts := <-seekChan:
					currTimestamp = ts
				default:
					continue
				}
			default:
				continue
			}
		}
		iter = GetOpLogQuery(s, currTimestamp, options).Tail(duration)
	}
	return nil
}

func FetchDocuments(session *mgo.Session, inOp OpChan, inErr chan error,
	outOp OpChan, outErr chan error) error {
	s := session.Copy()
	defer s.Close()
	for {
		select {
		case err := <-inErr:
			outErr <- err
		case op := <-inOp:
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

func DefaultOptions() *Options {
	return &Options{
		After:               nil,
		Filter:              nil,
		OpLogDatabaseName:   nil,
		OpLogCollectionName: nil,
		CursorTimeout:       nil,
		ChannelSize:         20,
	}
}

func Tail(session *mgo.Session, options *Options) (OpChan, chan error) {
	if options == nil {
		options = DefaultOptions()
	} else if options.ChannelSize < 1 {
		options.ChannelSize = 20
	}
	inErr := make(chan error, options.ChannelSize)
	outErr := make(chan error, options.ChannelSize)
	inOp := make(OpChan, options.ChannelSize)
	outOp := make(OpChan, options.ChannelSize)
	go FetchDocuments(session, inOp, inErr, outOp, outErr)
	go TailOps(session, inOp, inErr, options)
	return outOp, outErr
}
