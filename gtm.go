package gtm

import (
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/pkg/errors"
	"github.com/serialx/hashring"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type OrderingGuarantee int

const (
	Oplog     OrderingGuarantee = iota // ops sent in oplog order (strong ordering)
	Namespace                          // ops sent in oplog order within a namespace
	Document                           // ops sent in oplog order for a single document
	AnyOrder                           // ops sent as they become available
)

type QuerySource int

const (
	OplogQuerySource QuerySource = iota
	DirectQuerySource
)

type Options struct {
	After               TimestampGenerator
	Filter              OpFilter
	NamespaceFilter     OpFilter
	OpLogDisabled       bool
	OpLogDatabaseName   *string
	OpLogCollectionName *string
	CursorTimeout       *string // deprecated
	ChannelSize         int
	BufferSize          int
	BufferDuration      time.Duration
	Ordering            OrderingGuarantee
	WorkerCount         int
	UpdateDataAsDelta   bool
	ChangeStreamNs      []string
	DirectReadNs        []string
	DirectReadFilter    OpFilter
	DirectReadSplitMax  int
	Unmarshal           DataUnmarshaller
	Pipe                PipelineBuilder
	PipeAllowDisk       bool
	SplitVector         bool
	Log                 *log.Logger
}

type Op struct {
	Id        interface{}            `json:"_id"`
	Operation string                 `json:"operation"`
	Namespace string                 `json:"namespace"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Timestamp bson.MongoTimestamp    `json:"timestamp"`
	Source    QuerySource            `json:"source"`
	Doc       interface{}            `json:"doc,omitempty"`
}

type OpLog struct {
	Timestamp    bson.MongoTimestamp "ts"
	HistoryID    int64               "h"
	MongoVersion int                 "v"
	Operation    string              "op"
	Namespace    string              "ns"
	Doc          *bson.Raw           "o"
	Update       *bson.Raw           "o2"
}

type SplitVectorResult struct {
	SplitKeys []bson.M "splitKeys"
	Ok        int      "ok"
}

type SplitVectorRequest struct {
	SplitVector    string      "splitVector"
	KeyPattern     bson.M      "keyPattern"
	Min            interface{} "min"
	Max            interface{} "max"
	MaxChunkSize   int         "maxChunkSize"
	MaxSplitPoints int         "maxSplitPoints"
	Force          bool        "force"
}

type ChangeDoc struct {
	DocKey    map[string]interface{} "documentKey"
	Id        interface{}            "_id"
	Operation string                 "operationType"
	FullDoc   *bson.Raw              "fullDocument"
	Namespace map[string]string      "ns"
	Timestamp bson.MongoTimestamp    "clusterTime"
}

func (cd *ChangeDoc) docId() interface{} {
	return cd.DocKey["_id"]
}

func (cd *ChangeDoc) mapTimestamp() bson.MongoTimestamp {
	if cd.Timestamp > 0 {
		// only supported in version 4.0
		return cd.Timestamp
	} else {
		t := time.Now().UTC().Unix()
		return bson.MongoTimestamp(t << 32)
	}
}

func (cd *ChangeDoc) mapOperation() string {
	if cd.Operation == "insert" {
		return "i"
	} else if cd.Operation == "update" || cd.Operation == "replace" {
		return "u"
	} else if cd.Operation == "delete" {
		return "d"
	} else if cd.Operation == "invalidate" {
		return "c"
	} else {
		return ""
	}
}

func (cd *ChangeDoc) hasDoc() bool {
	return (cd.mapOperation() == "i" || cd.mapOperation() == "u") && cd.FullDoc != nil
}

func (cd *ChangeDoc) isInvalidate() bool {
	return cd.Operation == "invalidate"
}

func (cd *ChangeDoc) mapNs() string {
	return cd.Namespace["db"] + "." + cd.Namespace["coll"]
}

type Doc struct {
	Id interface{} "_id"
}

type CollectionStats struct {
	Count         int64 "count"
	AvgObjectSize int64 "avgObjSize"
}

type CollectionSegment struct {
	min         interface{}
	max         interface{}
	splitKey    string
	splits      []bson.M
	subSegments []*CollectionSegment
}

func (cs *CollectionSegment) shrinkTo(next interface{}) {
	cs.max = next
}

func (cs *CollectionSegment) toSelector() bson.M {
	var sel bson.M
	rang := bson.M{}
	if cs.max != nil {
		rang["$lt"] = cs.max
	}
	if cs.min != nil {
		rang["$gte"] = cs.min
	}
	if len(rang) > 0 {
		sel = bson.M{}
		sel[cs.splitKey] = rang
	}
	return sel
}

func (cs *CollectionSegment) divide() {
	if len(cs.splits) == 0 {
		return
	}
	ns := &CollectionSegment{
		splitKey: cs.splitKey,
		min:      cs.min,
		max:      cs.max,
	}
	cs.subSegments = nil
	for _, split := range cs.splits {
		ns.shrinkTo(split[cs.splitKey])
		cs.subSegments = append(cs.subSegments, ns)
		ns = &CollectionSegment{
			splitKey: cs.splitKey,
			min:      ns.max,
			max:      cs.max,
		}
	}
	ns = &CollectionSegment{
		splitKey: cs.splitKey,
		min:      cs.splits[len(cs.splits)-1][cs.splitKey],
	}
	cs.subSegments = append(cs.subSegments, ns)
}

func (cs *CollectionSegment) init(c *mgo.Collection) (err error) {
	doc := bson.M{}
	var q *mgo.Query
	q = c.Find(nil).Sort(cs.splitKey).Limit(1)
	if err = q.One(&doc); err != nil {
		return
	}
	cs.min = doc[cs.splitKey]
	q = c.Find(nil).Sort("-" + cs.splitKey).Limit(1)
	if err = q.One(&doc); err != nil {
		return
	}
	cs.max = doc[cs.splitKey]
	return
}

type OpChan chan *Op

type OpLogEntry map[string]interface{}

type OpFilter func(*Op) bool

type ShardInsertHandler func(*ShardInfo) (*mgo.Session, error)

type TimestampGenerator func(*mgo.Session, *Options) bson.MongoTimestamp

type DataUnmarshaller func(namespace string, raw *bson.Raw) (interface{}, error)

type PipelineBuilder func(namespace string, changeStream bool) ([]interface{}, error)

type OpBuf struct {
	Entries        []*Op
	BufferSize     int
	BufferDuration time.Duration
	FlushTicker    *time.Ticker
}

type OpCtx struct {
	lock         *sync.Mutex
	OpC          OpChan
	ErrC         chan error
	DirectReadWg *sync.WaitGroup
	stopC        chan bool
	allWg        *sync.WaitGroup
	seekC        chan bson.MongoTimestamp
	pauseC       chan bool
	resumeC      chan bool
	paused       bool
	stopped      bool
	log          *log.Logger
	streams      int
}

type OpCtxMulti struct {
	lock         *sync.Mutex
	contexts     []*OpCtx
	OpC          OpChan
	ErrC         chan error
	DirectReadWg *sync.WaitGroup
	opWg         *sync.WaitGroup
	stopC        chan bool
	allWg        *sync.WaitGroup
	seekC        chan bson.MongoTimestamp
	pauseC       chan bool
	resumeC      chan bool
	paused       bool
	stopped      bool
	log          *log.Logger
}

type ShardInfo struct {
	hostname string
}

type BuildInfo struct {
	version []int
	major   int
	minor   int
	patch   int
}

type N struct {
	database   string
	collection string
}

func (b *BuildInfo) build() {
	parts := len(b.version)
	if parts > 0 {
		b.major = b.version[0]
	}
	if parts > 1 {
		b.minor = b.version[1]
	}
	if parts > 2 {
		b.patch = b.version[2]
	}
}

func (n *N) parse(ns string) (err error) {
	parts := strings.SplitN(ns, ".", 2)
	if len(parts) != 2 {
		err = fmt.Errorf("Invalid ns: %s :expecting db.collection", ns)
	} else {
		n.database = parts[0]
		n.collection = parts[1]
	}
	return
}

func (shard *ShardInfo) GetURL() string {
	hostParts := strings.SplitN(shard.hostname, "/", 2)
	if len(hostParts) == 2 {
		return hostParts[1] + "?replicaSet=" + hostParts[0]
	} else {
		return hostParts[0]
	}
}

func (ctx *OpCtx) waitForConnection(wg *sync.WaitGroup, session *mgo.Session, options *Options) {
	defer wg.Done()
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.stopC:
			return
		case <-t.C:
			s := session.Copy()
			if err := s.Ping(); err == nil {
				s.Close()
				return
			}
			s.Close()
		}
	}
}

func (ctx *OpCtx) isStopped() bool {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	return ctx.stopped
}

func (ctx *OpCtx) Since(ts bson.MongoTimestamp) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	for i := 0; i < ctx.streams; i++ {
		ctx.seekC <- ts
	}
}

func (ctx *OpCtx) Pause() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if !ctx.paused {
		ctx.paused = true
		for i := 0; i < ctx.streams; i++ {
			ctx.pauseC <- true
		}
	}
}

func (ctx *OpCtx) Resume() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if ctx.paused {
		ctx.paused = false
		for i := 0; i < ctx.streams; i++ {
			ctx.resumeC <- true
		}
	}
}

func (ctx *OpCtx) Stop() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if !ctx.stopped {
		ctx.stopped = true
		close(ctx.stopC)
		ctx.allWg.Wait()
		close(ctx.OpC)
		close(ctx.ErrC)
	}
}

func (ctx *OpCtxMulti) Since(ts bson.MongoTimestamp) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	for _, child := range ctx.contexts {
		child.Since(ts)
	}
}

func (ctx *OpCtxMulti) Pause() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if !ctx.paused {
		ctx.paused = true
		ctx.pauseC <- true
		for _, child := range ctx.contexts {
			child.Pause()
		}
	}
}

func (ctx *OpCtxMulti) Resume() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if ctx.paused {
		ctx.paused = false
		ctx.resumeC <- true
		for _, child := range ctx.contexts {
			child.Resume()
		}
	}
}

func (ctx *OpCtxMulti) Stop() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if !ctx.stopped {
		ctx.stopped = true
		close(ctx.stopC)
		for _, child := range ctx.contexts {
			go child.Stop()
		}
		ctx.allWg.Wait()
		close(ctx.OpC)
		close(ctx.ErrC)
		ctx.opWg.Wait()
	}
}

func tailShards(multi *OpCtxMulti, ctx *OpCtx, options *Options, handler ShardInsertHandler) {
	defer multi.allWg.Done()
	if options == nil {
		options = DefaultOptions()
	} else {
		options.SetDefaults()
	}
	for {
		select {
		case <-multi.stopC:
			return
		case <-multi.pauseC:
			<-multi.resumeC
			select {
			case <-multi.stopC:
				return
			}
		case err := <-ctx.ErrC:
			if err == nil {
				break
			}
			multi.ErrC <- err
		case op := <-ctx.OpC:
			if op == nil {
				break
			}
			// new shard detected
			shardInfo := &ShardInfo{
				hostname: op.Data["host"].(string),
			}
			shardSession, err := handler(shardInfo)
			if err != nil {
				multi.ErrC <- errors.Wrap(err, "Error calling shard handler")
				continue
			}
			shardCtx := Start(shardSession, options)
			multi.lock.Lock()
			multi.contexts = append(multi.contexts, shardCtx)
			multi.DirectReadWg.Add(1)
			multi.allWg.Add(1)
			multi.opWg.Add(2)
			go func() {
				defer multi.DirectReadWg.Done()
				shardCtx.DirectReadWg.Wait()
			}()
			go func() {
				defer multi.allWg.Done()
				shardCtx.allWg.Wait()
			}()
			go func(c OpChan) {
				defer multi.opWg.Done()
				for op := range c {
					multi.OpC <- op
				}
			}(shardCtx.OpC)
			go func(c chan error) {
				defer multi.opWg.Done()
				for err := range c {
					multi.ErrC <- err
				}
			}(shardCtx.ErrC)
			multi.lock.Unlock()
		}
	}
}

func (ctx *OpCtxMulti) AddShardListener(
	configSession *mgo.Session, shardOptions *Options, handler ShardInsertHandler) {
	opts := DefaultOptions()
	if shardOptions != nil && shardOptions.OpLogDisabled {
		opts.ChangeStreamNs = []string{"config.shards"}
		opts.NamespaceFilter = func(op *Op) bool {
			return op.IsInsert()
		}
	} else {
		opts.NamespaceFilter = func(op *Op) bool {
			return op.Namespace == "config.shards" && op.IsInsert()
		}
	}
	configCtx := Start(configSession, opts)
	ctx.allWg.Add(1)
	go tailShards(ctx, configCtx, shardOptions, handler)
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

func (this *Op) IsSourceOplog() bool {
	return this.Source == OplogQuerySource
}

func (this *Op) IsSourceDirect() bool {
	return this.Source == DirectQuerySource
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

func (this *OpBuf) Append(op *Op) {
	this.Entries = append(this.Entries, op)
}

func (this *OpBuf) IsFull() bool {
	return len(this.Entries) >= this.BufferSize
}

func (this *OpBuf) Flush(session *mgo.Session, ctx *OpCtx, options *Options) {
	if len(this.Entries) == 0 {
		return
	}
	ns := make(map[string][]interface{})
	byId := make(map[interface{}][]*Op)
	for _, op := range this.Entries {
		if op.IsUpdate() && op.Doc == nil {
			idKey := fmt.Sprintf("%s.%v", op.Namespace, op.Id)
			ns[op.Namespace] = append(ns[op.Namespace], op.Id)
			byId[idKey] = append(byId[idKey], op)
		}
	}
Retry:
	for n, opIds := range ns {
		var parts = strings.SplitN(n, ".", 2)
		var results []*bson.Raw
		db, col := parts[0], parts[1]
		sel := bson.M{"_id": bson.M{"$in": opIds}}
		collection := session.DB(db).C(col)
		err := collection.Find(sel).All(&results)
		if err == nil {
			for _, result := range results {
				var doc Doc
				result.Unmarshal(&doc)
				resultId := fmt.Sprintf("%s.%v", n, doc.Id)
				if ops, ok := byId[resultId]; ok {
					for _, o := range ops {
						if u, err := options.Unmarshal(o.Namespace, result); err == nil {
							o.processData(u)
						} else {
							ctx.ErrC <- err
						}
					}
				}
			}
		} else {
			ctx.ErrC <- errors.Wrap(err, "Error finding documents to associate with ops")
			var wg sync.WaitGroup
			wg.Add(1)
			go ctx.waitForConnection(&wg, session, options)
			wg.Wait()
			if ctx.isStopped() {
				this.Entries = nil
				return
			}
			session.Refresh()
			break Retry
		}
	}
	for _, op := range this.Entries {
		if op.matchesFilter(options) {
			ctx.OpC <- op
		}
	}
	this.Entries = nil
}

func UpdateIsReplace(entry map[string]interface{}) bool {
	if _, ok := entry["$set"]; ok {
		return false
	} else if _, ok := entry["$unset"]; ok {
		return false
	} else {
		return true
	}
}

func (this *Op) shouldParse() bool {
	return this.IsInsert() || this.IsDelete() || this.IsUpdate() || this.IsCommand()
}

func (this *Op) matchesNsFilter(options *Options) bool {
	return options.NamespaceFilter == nil || options.NamespaceFilter(this)
}

func (this *Op) matchesFilter(options *Options) bool {
	return options.Filter == nil || options.Filter(this)
}

func (this *Op) matchesDirectFilter(options *Options) bool {
	return options.DirectReadFilter == nil || options.DirectReadFilter(this)
}

func (this *Op) processData(data interface{}) {
	if data != nil {
		this.Doc = data
		if m, ok := data.(map[string]interface{}); ok {
			this.Data = m
		}
	}
}

func (this *Op) ParseLogEntry(entry *OpLog, options *Options) (include bool, err error) {
	var rawField *bson.Raw
	var u interface{}
	this.Operation = entry.Operation
	this.Timestamp = entry.Timestamp
	this.Namespace = entry.Namespace
	if this.shouldParse() {
		if this.IsCommand() {
			var objectField map[string]interface{}
			rawField = entry.Doc
			err = rawField.Unmarshal(&objectField)
			this.processData(objectField)
		}
		if this.matchesNsFilter(options) {
			if this.IsInsert() || this.IsDelete() || this.IsUpdate() {
				if this.IsUpdate() {
					rawField = entry.Update
				} else {
					rawField = entry.Doc
				}
				var doc Doc
				rawField.Unmarshal(&doc)
				this.Id = doc.Id
				if this.IsInsert() || this.IsDelete() {
					if u, err = options.Unmarshal(this.Namespace, rawField); err == nil {
						this.processData(u)
					}
				} else if this.IsUpdate() {
					var changeField map[string]interface{}
					rawField = entry.Doc
					rawField.Unmarshal(&changeField)
					if options.UpdateDataAsDelta || UpdateIsReplace(changeField) {
						if u, err = options.Unmarshal(this.Namespace, rawField); err == nil {
							this.processData(u)
						}
					}
				}
				include = true
			} else if this.IsCommand() {
				include = this.IsDrop()
			}
		}
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
		for database %v: %s`, *options.OpLogDatabaseName, err)
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
	query := bson.M{"ts": bson.M{"$gt": after}, "fromMigrate": bson.M{"$exists": false}}
	collection := OpLogCollection(session, options)
	return collection.Find(query).LogReplay().Sort("$natural")
}

func opDataReady(op *Op, options *Options) (ready bool) {
	if options.UpdateDataAsDelta {
		ready = true
	} else if options.Ordering == AnyOrder {
		if op.IsUpdate() {
			ready = op.Data != nil || op.Doc != nil
		} else {
			ready = true
		}
	}
	return
}

func TailOps(ctx *OpCtx, session *mgo.Session, channels []OpChan, options *Options) error {
	defer ctx.allWg.Done()
	s := session.Copy()
	s.SetCursorTimeout(0) // keep this cursor alive
	defer s.Close()
	options.Fill(s)
	currTimestamp := options.After(s, options)
	var iter *mgo.Iter
	var err error
	sendError := func(err error) {
		ctx.ErrC <- errors.Wrap(err, "Error tailing oplog entries")
	}
Seek:
	for {
		var entry OpLog
		if iter != nil {
			if err = iter.Close(); err != nil {
				sendError(err)
				var wg sync.WaitGroup
				wg.Add(1)
				go ctx.waitForConnection(&wg, s, options)
				wg.Wait()
				if ctx.isStopped() {
					return nil
				}
				s.Refresh()
			}
		}
		iter = GetOpLogQuery(s, currTimestamp, options).Tail(-1)
	retry:
		for iter.Next(&entry) {
			op := &Op{
				Id:        "",
				Operation: "",
				Namespace: "",
				Data:      nil,
				Timestamp: bson.MongoTimestamp(0),
				Source:    OplogQuerySource,
			}
			ok, err := op.ParseLogEntry(&entry, options)
			if err == nil {
				if ok && op.matchesFilter(options) {
					if opDataReady(op, options) {
						ctx.OpC <- op
					} else {
						// broadcast to fetch channels
						for _, channel := range channels {
							channel <- op
						}
					}
				}
			} else {
				sendError(err)
			}
			select {
			case <-ctx.stopC:
				if err = iter.Close(); err != nil {
					sendError(err)
				}
				return nil
			case ts := <-ctx.seekC:
				currTimestamp = ts
				break Seek
			case <-ctx.pauseC:
				<-ctx.resumeC
				select {
				case <-ctx.stopC:
					if err = iter.Close(); err != nil {
						sendError(err)
					}
					return nil
				case ts := <-ctx.seekC:
					currTimestamp = ts
					break Seek
				default:
					currTimestamp = op.Timestamp
				}
			default:
				currTimestamp = op.Timestamp
			}
		}
		if iter.Timeout() {
			select {
			case <-ctx.stopC:
				if err = iter.Close(); err != nil {
					sendError(err)
				}
				return nil
			default:
				goto retry
			}
		}
	}
	return nil
}

func DirectReadSplitVector(ctx *OpCtx, session *mgo.Session, ns string, options *Options) (err error) {
	defer ctx.allWg.Done()
	defer ctx.DirectReadWg.Done()
	doPagedRead := func() {
		ctx.allWg.Add(1)
		ctx.DirectReadWg.Add(1)
		go DirectReadPaged(ctx, session, ns, options)
	}
	n := &N{}
	if err = n.parse(ns); err != nil {
		ctx.ErrC <- errors.Wrap(err, "Error starting direct reads. Invalid namespace.")
		return
	}
	var stats *CollectionStats
	stats, _ = GetCollectionStats(ctx, session, ns)
	col := session.DB(n.database).C(n.collection)
	indexes, err := col.Indexes()
	if err != nil {
		msg := fmt.Sprintf("Unable to determine indexes on %s for direct read split vector", ns)
		ctx.ErrC <- errors.Wrap(err, msg)
		doPagedRead()
		return
	}
	var maxSplits = options.DirectReadSplitMax
	var splitMax, splitMin int
	splitMin = 4
	bestSplit := &CollectionSegment{
		splitKey: "_id",
	}
	for _, index := range indexes {
		key := strings.TrimPrefix(index.Key[0], "-")
		if key == "_id" {
			continue
		}
		if splitMax >= maxSplits {
			break
		}
		cseg := &CollectionSegment{
			splitKey: key,
		}
		err = cseg.init(col)
		if err == nil {
			dir := 1
			if strings.HasPrefix(index.Key[0], "-") {
				dir = -1
			}
			splitv := SplitVectorRequest{
				SplitVector:    ns,
				KeyPattern:     bson.M{cseg.splitKey: dir},
				Min:            cseg.min,
				Max:            cseg.max,
				MaxChunkSize:   8,
				MaxSplitPoints: maxSplits,
			}
			var result SplitVectorResult
			err = session.Run(splitv, &result)
			if err != nil || result.Ok == 0 {
				msg := fmt.Sprintf("Split Vector admin command failed for key pattern %s in namespace %s", ns, key)
				ctx.ErrC <- errors.Wrap(err, msg)
				continue
			}
			curSplits := len(result.SplitKeys)
			if curSplits > splitMax {
				splitMax = curSplits
				bestSplit = cseg
				bestSplit.splits = result.SplitKeys
			}
		} else {
			msg := fmt.Sprintf("Unable to check index bounds for namespace %s using key pattern %s", ns, key)
			ctx.ErrC <- errors.Wrap(err, msg)
		}
	}
	if splitMax < splitMin {
		doPagedRead()
	} else {
		ctx.log.Printf("Found %d splits (%d segments) for namespace %s using index on %s", splitMax, splitMax+1, ns, bestSplit.splitKey)
		bestSplit.divide()
		if len(bestSplit.subSegments) > 0 {
			for _, subseg := range bestSplit.subSegments {
				ctx.allWg.Add(1)
				ctx.DirectReadWg.Add(1)
				go DirectReadSegment(ctx, session, ns, options, subseg, stats)
			}
		} else {
			doPagedRead()
		}
	}
	return
}

func notSupportedOnView(err error) bool {
	switch e := err.(type) {
	case *mgo.LastError:
		return e.Code == 166 || e.Code == 167
	case *mgo.QueryError:
		return e.Code == 166 || e.Code == 167
	}
	return false
}

func GetCollectionStats(ctx *OpCtx, session *mgo.Session, ns string) (stats *CollectionStats, err error) {
	stats = &CollectionStats{}
	n := &N{}
	if err = n.parse(ns); err != nil {
		ctx.ErrC <- errors.Wrap(err, "Error starting direct reads. Invalid namespace.")
		return
	}
	err = session.DB(n.database).Run(bson.D{{"collStats", n.collection}}, stats)
	return
}

func DirectReadSegment(ctx *OpCtx, session *mgo.Session, ns string, options *Options, seg *CollectionSegment, stats *CollectionStats) (err error) {
	defer ctx.allWg.Done()
	defer ctx.DirectReadWg.Done()
	s := session.Copy()
	s.SetMode(mgo.Nearest, true)
	defer s.Close()
	n := &N{}
	if err = n.parse(ns); err != nil {
		ctx.ErrC <- errors.Wrap(err, "Error starting direct reads. Invalid namespace.")
		return
	}
	var batch int64 = 1000
	if stats.AvgObjectSize != 0 {
		batch = (8 * 1024 * 1024) / stats.AvgObjectSize // 8MB divided by avg doc size
		if batch < 1000 {
			// leave it up to the server
			batch = 0
		}
	}
	var iter *mgo.Iter
	c := s.DB(n.database).C(n.collection)
	sel := seg.toSelector()
	q := c.Find(sel)
	if batch != 0 {
		q.Batch(int(batch))
	}
	iter = q.Iter()
	if options.Pipe != nil {
		var pipeline []interface{}
		if pipeline, err = options.Pipe(ns, false); err != nil {
			ctx.ErrC <- errors.Wrap(err, "Error building aggregation pipeline stages.")
			return
		}
		if pipeline != nil {
			var stages []interface{}
			stages = append(stages, bson.M{"$match": sel})
			for _, stage := range pipeline {
				stages = append(stages, stage)
			}
			pipe := c.Pipe(stages)
			if options.PipeAllowDisk {
				pipe = pipe.AllowDiskUse()
			}
			iter = pipe.Iter()
		}
	}
retry:
	var result = &bson.Raw{}
	for iter.Next(&result) {
		var doc Doc
		result.Unmarshal(&doc)
		t := time.Now().UTC().Unix()
		op := &Op{
			Id:        doc.Id,
			Operation: "i",
			Namespace: ns,
			Source:    DirectQuerySource,
			Timestamp: bson.MongoTimestamp(t << 32),
		}
		if u, err := options.Unmarshal(ns, result); err == nil {
			op.processData(u)
			if op.matchesDirectFilter(options) {
				ctx.OpC <- op
			}
		} else {
			ctx.ErrC <- err
		}
		result = &bson.Raw{}
		select {
		case <-ctx.stopC:
			iter.Close()
			return
		default:
			continue
		}
	}
	if iter.Timeout() {
		select {
		case <-ctx.stopC:
			iter.Close()
			return
		default:
			goto retry
		}
	}
	if err = iter.Close(); err != nil {
		ctx.ErrC <- errors.Wrap(err, fmt.Sprintf("Error reading segment of collection %s. Will retry segment.", ns))
		var wg sync.WaitGroup
		wg.Add(1)
		go ctx.waitForConnection(&wg, s, options)
		wg.Wait()
		if ctx.isStopped() {
			return
		}
		ctx.allWg.Add(1)
		ctx.DirectReadWg.Add(1)
		go DirectReadSegment(ctx, session, ns, options, seg, stats)
	}
	return
}

func ConsumeChangeStream(ctx *OpCtx, session *mgo.Session, ns string, options *Options) (err error) {
	defer ctx.allWg.Done()
	s := session.Copy()
	defer s.Close()
	n := &N{}
	if err = n.parse(ns); err != nil {
		ctx.ErrC <- errors.Wrap(err, "Error consuming change stream. Invalid namespace.")
		return
	}
	c := s.DB(n.database).C(n.collection)
	var pipeline []interface{}
	var token *bson.Raw
	if options.Pipe != nil {
		var stages []interface{}
		if stages, err = options.Pipe(ns, true); err != nil {
			ctx.ErrC <- errors.Wrap(err, "Error building aggregation pipeline stages.")
			return
		}
		if stages != nil {
			pipeline = stages
		}
	}
	for {
		var stream *mgo.ChangeStream
		stream, err = c.Watch(pipeline, mgo.ChangeStreamOptions{ResumeAfter: token, FullDocument: "updateLookup"})
		if err != nil {
			ctx.ErrC <- errors.Wrap(err, "Error consuming change stream.")
			return
		}
	retry:
		var changeDoc ChangeDoc
		for stream.Next(&changeDoc) {
			if changeDoc.isInvalidate() {
				op := &Op{
					Operation: changeDoc.mapOperation(),
					Namespace: ns,
					Source:    OplogQuerySource,
					Timestamp: changeDoc.mapTimestamp(),
				}
				op.Data = map[string]interface{}{"drop": n.collection}
				ctx.OpC <- op
				stream.Close()
				return
			} else {
				op := &Op{
					Id:        changeDoc.docId(),
					Operation: changeDoc.mapOperation(),
					Namespace: ns,
					Source:    OplogQuerySource,
					Timestamp: changeDoc.mapTimestamp(),
				}
				if changeDoc.hasDoc() {
					if u, err := options.Unmarshal(ns, changeDoc.FullDoc); err == nil {
						op.processData(u)
						if op.matchesDirectFilter(options) {
							ctx.OpC <- op
						}
					} else {
						ctx.ErrC <- err
					}
				} else {
					if op.matchesDirectFilter(options) {
						ctx.OpC <- op
					}
				}
			}
			select {
			case <-ctx.stopC:
				stream.Close()
				return
			case <-ctx.pauseC:
				<-ctx.resumeC
				select {
				case <-ctx.stopC:
					stream.Close()
					return
				default:
					continue
				}
			default:
				continue
			}
		}
		if stream.Timeout() {
			select {
			case <-ctx.stopC:
				stream.Close()
				return
			default:
				goto retry
			}
		}
		token = stream.ResumeToken()
		if err := stream.Close(); err != nil {
			ctx.ErrC <- errors.Wrap(err, "Error while consuming change stream.")
		}
	}
}

func DirectReadPaged(ctx *OpCtx, session *mgo.Session, ns string, options *Options) (err error) {
	defer ctx.allWg.Done()
	defer ctx.DirectReadWg.Done()
	s := session.Copy()
	defer s.Close()
	n := &N{}
	if err = n.parse(ns); err != nil {
		ctx.ErrC <- errors.Wrap(err, "Error starting direct reads. Invalid namespace.")
		return
	}
	var stats *CollectionStats
	stats, _ = GetCollectionStats(ctx, session, ns)
	c := s.DB(n.database).C(n.collection)
	const defaultSegmentSize = 50000
	var maxSplits = options.DirectReadSplitMax
	var segmentSize int = defaultSegmentSize
	if stats.Count != 0 {
		segmentSize = int(stats.Count) / (maxSplits + 1)
		if segmentSize < defaultSegmentSize {
			segmentSize = defaultSegmentSize
		}
	}
	segment := &CollectionSegment{
		splitKey: "_id",
	}
	var doc Doc
	var splitCount int
	pro := bson.M{"_id": 1}
	done := false
	for !done {
		sel := bson.M{}
		if segment.min != nil {
			sel["_id"] = bson.M{
				"$gte": segment.min,
			}
		}
		err = c.Find(sel).Select(pro).Sort("_id").Skip(segmentSize).One(&doc)
		if err == nil {
			segment.max = doc.Id
		} else {
			done = true
		}
		ctx.allWg.Add(1)
		ctx.DirectReadWg.Add(1)
		go DirectReadSegment(ctx, session, ns, options, segment, stats)
		if !done {
			segment = &CollectionSegment{
				splitKey: "_id",
				min:      segment.max,
			}
			splitCount = splitCount + 1
			if splitCount == maxSplits {
				done = true
				ctx.allWg.Add(1)
				ctx.DirectReadWg.Add(1)
				go DirectReadSegment(ctx, session, ns, options, segment, stats)
			}
		}
	}
	return
}

func FetchDocuments(ctx *OpCtx, session *mgo.Session, filter OpFilter, buf *OpBuf, inOp OpChan, options *Options) error {
	defer ctx.allWg.Done()
	s := session.Copy()
	defer s.Close()
	for {
		select {
		case <-ctx.stopC:
			return nil
		case <-buf.FlushTicker.C:
			buf.Flush(s, ctx, options)
		case op := <-inOp:
			if op == nil {
				break
			}
			if filter(op) {
				buf.Append(op)
				if buf.IsFull() {
					buf.Flush(s, ctx, options)
					buf.FlushTicker.Stop()
					buf.FlushTicker = time.NewTicker(buf.BufferDuration)
				}
			}
		}
	}
	return nil
}

func OpFilterForOrdering(ordering OrderingGuarantee, workers []string, worker string) OpFilter {
	switch ordering {
	case AnyOrder, Document:
		ring := hashring.New(workers)
		return func(op *Op) bool {
			var key string
			if op.Id != nil {
				key = fmt.Sprintf("%v", op.Id)
			} else {
				key = op.Namespace
			}
			if who, ok := ring.GetNode(key); ok {
				return who == worker
			} else {
				return false
			}
		}
	case Namespace:
		ring := hashring.New(workers)
		return func(op *Op) bool {
			if who, ok := ring.GetNode(op.Namespace); ok {
				return who == worker
			} else {
				return false
			}
		}
	default:
		return func(op *Op) bool {
			return true
		}
	}
}

func DefaultOptions() *Options {
	return &Options{
		After:               nil,
		Filter:              nil,
		NamespaceFilter:     nil,
		OpLogDatabaseName:   nil,
		OpLogCollectionName: nil,
		OpLogDisabled:       false,
		ChannelSize:         2048,
		BufferSize:          50,
		BufferDuration:      time.Duration(75) * time.Millisecond,
		Ordering:            Oplog,
		WorkerCount:         10,
		UpdateDataAsDelta:   false,
		DirectReadNs:        []string{},
		DirectReadFilter:    nil,
		DirectReadSplitMax:  9,
		Unmarshal:           defaultUnmarshaller,
		SplitVector:         false,
		Log:                 log.New(os.Stdout, "INFO ", log.Flags()),
	}
}

func (this *Options) Fill(session *mgo.Session) {
	if this.After == nil {
		this.After = LastOpTimestamp
	}
	if this.OpLogDatabaseName == nil {
		defaultOpLogDatabaseName := "local"
		this.OpLogDatabaseName = &defaultOpLogDatabaseName
	}
	if this.OpLogCollectionName == nil {
		defaultOpLogCollectionName := OpLogCollectionName(session, this)
		this.OpLogCollectionName = &defaultOpLogCollectionName
	}
}

func defaultUnmarshaller(namespace string, raw *bson.Raw) (interface{}, error) {
	var m map[string]interface{}
	if err := raw.Unmarshal(&m); err == nil {
		return m, nil
	} else {
		return nil, err
	}
}

func (this *Options) SetDefaults() {
	defaultOpts := DefaultOptions()
	if this.ChannelSize < 1 {
		this.ChannelSize = defaultOpts.ChannelSize
	}
	if this.BufferSize < 1 {
		this.BufferSize = defaultOpts.BufferSize
	}
	if this.BufferDuration == 0 {
		this.BufferDuration = defaultOpts.BufferDuration
	}
	if this.Ordering == Oplog {
		this.WorkerCount = 1
	}
	if this.WorkerCount < 1 {
		this.WorkerCount = 1
	}
	if this.UpdateDataAsDelta {
		this.Ordering = Oplog
		this.WorkerCount = 0
	}
	if this.Unmarshal == nil {
		this.Unmarshal = defaultOpts.Unmarshal
	}
	if this.Log == nil {
		this.Log = defaultOpts.Log
	}
	if this.DirectReadSplitMax < 1 {
		this.DirectReadSplitMax = defaultOpts.DirectReadSplitMax
	}
}

func Tail(session *mgo.Session, options *Options) (OpChan, chan error) {
	ctx := Start(session, options)
	return ctx.OpC, ctx.ErrC
}

func GetShards(session *mgo.Session) (shardInfos []*ShardInfo) {
	// use this for sharded databases to get the shard hosts
	// use the hostnames to create multiple sessions for a call to StartMulti
	col := session.DB("config").C("shards")
	var shards []map[string]interface{}
	col.Find(nil).All(&shards)
	for _, shard := range shards {
		host := shard["host"].(string)
		shardInfo := &ShardInfo{
			hostname: host,
		}
		shardInfos = append(shardInfos, shardInfo)
	}
	return
}

func VersionInfo(session *mgo.Session) (buildInfo *BuildInfo, err error) {
	if info, err := session.BuildInfo(); err == nil {
		buildInfo = &BuildInfo{
			version: info.VersionArray,
		}
		buildInfo.build()
	}
	return
}

func StartMulti(sessions []*mgo.Session, options *Options) *OpCtxMulti {
	if options == nil {
		options = DefaultOptions()
	} else {
		options.SetDefaults()
	}

	stopC := make(chan bool, 1)
	errC := make(chan error, options.ChannelSize)
	opC := make(OpChan, options.ChannelSize)

	var directReadWg sync.WaitGroup
	var opWg sync.WaitGroup
	var allWg sync.WaitGroup
	var seekC = make(chan bson.MongoTimestamp, 1)
	var pauseC = make(chan bool, 1)
	var resumeC = make(chan bool, 1)

	ctxMulti := &OpCtxMulti{
		lock:         &sync.Mutex{},
		OpC:          opC,
		ErrC:         errC,
		DirectReadWg: &directReadWg,
		opWg:         &opWg,
		stopC:        stopC,
		allWg:        &allWg,
		pauseC:       pauseC,
		resumeC:      resumeC,
		seekC:        seekC,
		log:          options.Log,
	}

	ctxMulti.lock.Lock()
	defer ctxMulti.lock.Unlock()

	for _, session := range sessions {
		ctx := Start(session, options)
		ctxMulti.contexts = append(ctxMulti.contexts, ctx)
		allWg.Add(1)
		directReadWg.Add(1)
		opWg.Add(2)
		go func() {
			defer directReadWg.Done()
			ctx.DirectReadWg.Wait()
		}()
		go func() {
			defer allWg.Done()
			ctx.allWg.Wait()
		}()
		go func(c OpChan) {
			defer opWg.Done()
			for op := range c {
				opC <- op
			}
		}(ctx.OpC)
		go func(c chan error) {
			defer opWg.Done()
			for err := range c {
				errC <- err
			}
		}(ctx.ErrC)
	}
	return ctxMulti
}

func Start(session *mgo.Session, options *Options) *OpCtx {
	if options == nil {
		options = DefaultOptions()
	} else {
		options.SetDefaults()
	}

	stopC := make(chan bool)
	errC := make(chan error, options.ChannelSize)
	opC := make(OpChan, options.ChannelSize)

	var inOps []OpChan
	var workerNames []string
	var directReadWg sync.WaitGroup
	var allWg sync.WaitGroup
	streams := len(options.ChangeStreamNs)
	if options.OpLogDisabled == false {
		streams += 1
	}
	var seekC = make(chan bson.MongoTimestamp, streams)
	var pauseC = make(chan bool, streams)
	var resumeC = make(chan bool, streams)

	ctx := &OpCtx{
		lock:         &sync.Mutex{},
		OpC:          opC,
		ErrC:         errC,
		DirectReadWg: &directReadWg,
		stopC:        stopC,
		allWg:        &allWg,
		pauseC:       pauseC,
		resumeC:      resumeC,
		seekC:        seekC,
		log:          options.Log,
		streams:      streams,
	}

	if options.OpLogDisabled == false {
		for i := 1; i <= options.WorkerCount; i++ {
			workerNames = append(workerNames, strconv.Itoa(i))
		}

		for i := 1; i <= options.WorkerCount; i++ {
			allWg.Add(1)
			inOp := make(OpChan, options.ChannelSize)
			inOps = append(inOps, inOp)
			buf := &OpBuf{
				BufferSize:     options.BufferSize,
				BufferDuration: options.BufferDuration,
				FlushTicker:    time.NewTicker(options.BufferDuration),
			}
			worker := strconv.Itoa(i)
			filter := OpFilterForOrdering(options.Ordering, workerNames, worker)
			go FetchDocuments(ctx, session, filter, buf, inOp, options)
		}
	}

	for _, ns := range options.DirectReadNs {
		directReadWg.Add(1)
		allWg.Add(1)
		if options.SplitVector {
			go DirectReadSplitVector(ctx, session, ns, options)
		} else {
			go DirectReadPaged(ctx, session, ns, options)
		}
	}

	for _, ns := range options.ChangeStreamNs {
		allWg.Add(1)
		go ConsumeChangeStream(ctx, session, ns, options)
	}

	if options.OpLogDisabled == false {
		allWg.Add(1)
		go TailOps(ctx, session, inOps, options)
	}

	return ctx
}
