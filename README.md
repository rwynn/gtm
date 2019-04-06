gtm
===
gtm (go tail mongo) is a utility written in Go which tails the MongoDB oplog and sends create, update, delete events to your code.
It can be used to send emails to new users, [index documents](https://www.github.com/rwynn/monstache), 
[write time series data](https://www.github.com/rwynn/mongofluxd), or something else.

### Other branches ###
If you are interested in trying the development version of gtm which targets the 
[offical golang driver](https://github.com/mongodb/mongo-go-driver)
from MongoDB then checkout the `leafny` branch.

### Requirements ###
+ [Go](http://golang.org/doc/install)
+ [globalsign/mgo](https://godoc.org/github.com/globalsign/mgo), a mongodb driver for Go
+ [mongodb](http://www.mongodb.org/)

### Installation ###

	go get github.com/rwynn/gtm

### Setup ###

gtm uses the MongoDB [oplog](https://docs.mongodb.com/manual/core/replica-set-oplog/) as an event source. 
You will need to ensure that MongoDB is configured to produce an oplog by 
[deploying a replica set](http://docs.mongodb.org/manual/tutorial/deploy-replica-set/).

If you haven't already done so, follow the 5 step 
[procedure](https://docs.mongodb.com/manual/tutorial/deploy-replica-set/#procedure) to initiate and 
validate your replica set. For local testing your replica set may contain a 
[single member](https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/).

### Usage ###

```golang
package main

import "github.com/globalsign/mgo"
import "github.com/globalsign/mgo/bson"
import "github.com/rwynn/gtm"
import "fmt"

func main() {
	// get a mgo session	
	session, err := mgo.Dial("localhost")
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	// nil options get initialized to gtm.DefaultOptions()
	ctx := gtm.Start(session, nil)
	// ctx.OpC is a channel to read ops from
	// ctx.ErrC is a channel to read errors from
	// ctx.Stop() stops all go routines started by gtm.Start
	for {
		// loop forever receiving events	
		select {
		case err := <-ctx.ErrC:
			// handle errors
			fmt.Println(err)
		case op := <-ctx.OpC:
			// op will be an insert, delete, update, or drop to mongo
			// you can check which by calling 
			// op.IsInsert(), op.IsDelete(), op.IsUpdate(), or op.IsDrop()

			// op.Data will get you the full document for inserts and updates

			// op.UpdateDescription will get you a map describing the set of changes
			// as shown in https://docs.mongodb.com/manual/reference/change-events/#update-event
			// op.UpdateDescription will only be available when you update docs via $set, $push, $unset, etc.
			// if you replace the entire document using an update command, then you will NOT get
			// information in op.UpdateDescription. e.g. db.update(doc, {total: "replace"});

			msg := fmt.Sprintf(`Got op <%v> for object <%v> 
			in database <%v> 
			and collection <%v> 
			and data <%v> 
			and change <%v> 
			and timestamp <%v>`,
				op.Operation, op.Id, op.GetDatabase(),
				op.GetCollection(), op.Data, op.UpdateDescription, op.Timestamp)
			fmt.Println(msg) // or do something more interesting
		}
	}
}
```

### Configuration ###

```golang
func PipeBuilder(namespace string, changeStream bool) ([]interface{}, error) {

	// to build your pipelines for change events you will want to reference
	// the MongoDB reference for change events at 
	// https://docs.mongodb.com/manual/reference/change-events/

	// you will only receive changeStream == true when you configure gtm with
	// ChangeStreamNS (requies MongoDB 3.6+).  You cannot build pipelines for
	// changes using legacy direct oplog tailing

	if namespace == "users.users" {
		// given a set of docs like {username: "joe", email: "joe@email.com", amount: 1}
		if changeStream {
			return []interface{}{
				bson.M{"$match": bson.M{"fullDocument.username": "joe"}},
			}, nil
		} else {
			return []interface{}{
				bson.M{"$match": bson.M{"username": "joe"}},
			}, nil
		}
	} else if namespace == "users.status" && changeStream {
		// return a pipeline that only receives events when a document is 
		// inserted, deleted, or a specific field is changed. In this case
		// only a change to field1 is processed.  Changes to other fields
		// do not match the pipeline query and thus you won't receive the event.
		return []interface{}{
			bson.M{"$match": bson.M{"$or": []interface{} {
				bson.M{"updateDescription": bson.M{"$exists": false}},
				bson.M{"updateDescription.updatedFields.field1": bson.M{"$exists": true}},
			}}},
		}, nil
	}
	return nil, nil
}

func NewUsers(op *gtm.Op) bool {
	return op.Namespace == "users.users" && op.IsInsert()
}

// if you want to listen only for certain events on certain collections
// pass a filter function in options
ctx := gtm.Start(session, &gtm.Options{
	NamespaceFilter: NewUsers, // only receive inserts in the user collection
})
// more options are available for tuning
ctx := gtm.Start(session, &gtm.Options{
	NamespaceFilter      nil,           // op filter function that has access to type/ns ONLY
	Filter               nil,           // op filter function that has access to type/ns/data
	After:               nil,     	    // if nil defaults to gtm.LastOpTimestamp; not yet supported for ChangeStreamNS
	OpLogDisabled:       false,         // true to disable tailing the MongoDB oplog
	OpLogDatabaseName:   nil,     	    // defaults to "local"
	OpLogCollectionName: nil,     	    // defaults to "oplog.rs"
	ChannelSize:         0,       	    // defaults to 20
	BufferSize:          25,            // defaults to 50. used to batch fetch documents on bursts of activity
	BufferDuration:      0,             // defaults to 750 ms. after this timeout the batch is force fetched
	WorkerCount:         8,             // defaults to 1. number of go routines batch fetching concurrently
	Ordering:            gtm.Document,  // defaults to gtm.Oplog. ordering guarantee of events on the output channel as compared to the oplog
	UpdateDataAsDelta:   false,         // set to true to only receive delta information in the Data field on updates (info straight from oplog)
	DirectReadNs:        []string{"db.users"}, // set to a slice of namespaces to read data directly from bypassing the oplog
	DirectReadSplitMax:  9,             // the max number of times to split a collection for concurrent reads (impacts memory consumption)
	Pipe:                PipeBuilder,   // an optional function to build aggregation pipelines
	PipeAllowDisk:       false,         // true to allow MongoDB to use disk for aggregation pipeline options with large result sets
	SplitVector:         false,         // whether or not to use internal MongoDB command split vector to split collections
	Log:                 myLogger,      // pass your own logger
	ChangeStreamNs       []string{"db.col1", "db.col2"}, // MongoDB 3.6+ only; set to a slice to namespaces to read via MongoDB change streams
})
```

### Direct Reads ###

If, in addition to tailing the oplog, you would like to also read entire collections you can set the DirectReadNs field
to a slice of MongoDB namespaces.  Documents from these collections will be read directly and output on the ctx.OpC channel.  

You can wait till all the collections have been fully read by using the DirectReadWg wait group on the ctx.

```golang
go func() {
	ctx.DirectReadWg.Wait()
	fmt.Println("direct reads are done")
}()
```

### Pause, Resume, Since, and Stop ###

You can pause, resume, or seek to a timestamp from the oplog. These methods effect only change events and not direct reads.

```golang
go func() {
	ctx.Pause()
	time.Sleep(time.Duration(2) * time.Minute)
	ctx.Resume()
	ctx.Since(previousTimestamp)
}()
```

You can stop all goroutines created by `Start` or `StartMulti`. You cannot resume a context once it has been stopped. You would need to create a new one.

```golang
go func() {
	ctx.Stop()
	fmt.Println("all go routines are stopped")
}
```

### Sharded Clusters ###

If you use `ChangeStreamNs` on MongoDB 3.6+ you can ignore this section.  Native change streams in MongoDB are shard aware.  You should connect to the
`mongos` routing server and change streams will work across shards.  This section is for those pre-3.6 that would like to read changes across all shards.

gtm has support for sharded MongoDB clusters.  You will want to start with a connection to the MongoDB config server to get the list of available shards.

```golang
// assuming the CONFIG server for a sharded cluster is running locally on port 27018
configSession, err = mgo.Dial("127.0.0.1:27018")
if err != nil {
panic(err)
}
// get the list of shard servers
shardInfos := gtm.GetShards(configSession)
```

for each shard you will create a session and append it to a slice of sessions

```golang
var shardSessions []*mgo.Session
// add each shard server to the sync list
for _, shardInfo := range shardInfos {
log.Printf("Adding shard found at %s\n", shardInfo.GetURL())
shardURL := shardInfo.GetURL()
shard, err := mgo.Dial(shardURL)
if err != nil {
    panic(err)
}
shardSessions = append(shardSessions, shard)
}
```

finally you will want to start a multi context.  The multi context behaves just like a single
context except that it tails multiple shard servers and coalesces the events to a single output
channel

```golang
multiCtx := gtm.StartMulti(shardSessions, nil)
```

after you have created the multi context for all the shards you can handle new shards being added
to the cluster at some later time by adding a listener. You will want to add this listener before 
you enter a loop to read events from the multi context.

```golang
insertHandler := func(shardInfo *gtm.ShardInfo) (*mgo.Session, error) {
	log.Printf("Adding shard found at %s\n", shardInfo.GetURL())
shardURL := shardInfo.GetURL()
return mgo.Dial(shardURL)
}

multiCtx.AddShardListener(configSession, nil, insertHandler)
```

### Custom Unmarshalling ###

If you'd like to unmarshall MongoDB documents into your own struct instead of the document getting
unmarshalled to a generic map[string]interface{} you can use a custom unmarshal function:

```golang
type MyDoc struct {
	Id interface{} "_id"
	Foo string "foo"
}

func custom(namespace string, raw *bson.Raw) (interface{}, error) {
	// use namespace, e.g. db.col, to map to a custom struct
	if namespace == "test.test" {
		var doc MyDoc
		if err := raw.Unmarshal(&doc); err == nil {
			return doc, nil
		} else {
			return nil, err
		}
	}
	return nil, errors.New("unsupported namespace")
}

ctx := gtm.Start(session, &gtm.Options{
	Unmarshal: custom,
}

for {
	select {
	case op:= <-ctx.OpC:
		if op.Namespace == "test.test" {
			doc := op.Doc.(MyDoc)
			fmt.Println(doc.Foo)
		}
	}
}
```

### Workers ###

You may want to distribute event handling between a set of worker processes on different machines.
To do this you can leverage the **github.com/rwynn/gtm/consistent** package.  

Create a TOML document containing a list of all the event handlers.

```toml
Workers = [ "Tom", "Dick", "Harry" ] 
```

Create a consistent filter to distribute the work between Tom, Dick, and Harry. A consistent filter
needs to acces the Data attribute of each op so it needs to be set as a Filter as opposed to a 
NamespaceFilter.

```golang
name := flag.String("name", "", "the name of this worker")
flag.Parse()
filter, filterErr := consistent.ConsistentHashFilterFromFile(*name, "/path/to/toml")
if filterErr != nil {
	panic(filterErr)
}

// there is also a method **consistent.ConsistentHashFilterFromDocument** which allows
// you to pass a Mongo document representing the config if you would like to avoid
// copying the same config file to multiple servers
```

Pass the filter into the options when calling gtm.Tail

```golang
ctx := gtm.Start(session, &gtm.Options{Filter: filter})
```

If you have your multiple filters you can use the gtm utility method ChainOpFilters

```golang
func ChainOpFilters(filters ...OpFilter) OpFilter
```

### Optimizing Direct Read Throughput with SplitVector enabled ###

If you enable SplitVector, to get the best througput possible on direct reads you will want to consider the indexes on your collections.  In the best
case scenario, for very large collections, you will have an index on a field with a moderately low cardinality.
For example, if you have 10 million documents in your collection and have a field named `category` that will have a
value between 1 and 20, and you have an index of this field, then gtm will be able to perform an `internal` MongoDB admin
command named `splitVector` on this key.  The results of the split vector command will return a sorted list of category split points.
Once gtm has the split points it is able to start splits+1 go routines with range queries to consume the entire collection concurrently. 
You will notice a line in the log like this is this is working.

	INFO 2018/04/24 18:23:23 Found 16 splits (17 segments) for namespace test.test using index on category

When this is working you will notice the connection count increase substancially in `mongostat`.  On the other hand, if you
do not have an index which yields a high number splits, gtm will force a split and it will only be able to start 2 go 
routines to read your collection concurrently. 

The user that gtm connects with will need to have admin access to perform the `splitVector` command.  If the user does not have
this access then gtm will use paginating range read of each collection.

Gtm previously supported the `parallelCollectionScan` command to get multiple read cursors on a collection.
However, this command only worked on the mmapv1 storage engine and will be `removed` completely once the mmapv1 engine is retired.
It looks like `splitVector` or something like it will be promoted in new versions on MongoDB.  

