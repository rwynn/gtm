gtm
===
gtm (go tail mongo) is a utility written in Go which tails the MongoDB oplog and sends create, update, delete events to your code.
It can be used to send emails to new users, [index documents](https://www.github.com/rwynn/monstache), 
[write time series data](https://www.github.com/rwynn/mongofluxd), or something else.

### Requirements ###
+ [Go](http://golang.org/doc/install)
+ [globalsign/mgo](https://godoc.org/github.com/globalsign/mgo), a mongodb driver for Go
+ [mongodb](http://www.mongodb.org/)
	+ Pass argument --master to mongod to ensure an oplog is created OR
	+ Setup [replica sets](http://docs.mongodb.org/manual/tutorial/deploy-replica-set/) to create oplog

### Installation ###

	go get github.com/rwynn/gtm

### Usage ###
	
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
			case err= <-ctx.ErrC:
				// handle errors
				fmt.Println(err)
			case op:= <-ctx.OpC:
				// op will be an insert, delete, update, or drop to mongo
				// you can check which by calling 
				// op.IsInsert(), op.IsDelete(), op.IsUpdate(), or op.IsDrop()
				// op.Data will get you the full document for inserts and updates
				msg := fmt.Sprintf(`Got op <%v> for object <%v> 
				in database <%v>
				and collection <%v>
				and data <%v>
				and timestamp <%v>`,
					op.Operation, op.Id, op.GetDatabase(),
					op.GetCollection(), op.Data, op.Timestamp)
				fmt.Println(msg) // or do something more interesting
			}
		}
	}

### Configuration ###

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
		After:               nil,     	    // if nil defaults to LastOpTimestamp
		OpLogDatabaseName:   nil,     	    // defaults to "local"
		OpLogCollectionName: nil,     	    // defaults to a collection prefixed "oplog."
		CursorTimeout:       nil,     	    // defaults to 100s
		ChannelSize:         0,       	    // defaults to 20
		BufferSize:          25,            // defaults to 50. used to batch fetch documents on bursts of activity
		BufferDuration:      0,             // defaults to 750 ms. after this timeout the batch is force fetched
		WorkerCount:         8,             // defaults to 1. number of go routines batch fetching concurrently
		Ordering:            gtm.Document,  // defaults to gtm.Oplog. ordering guarantee of events on the output channel
		UpdateDataAsDelta:   false,         // set to true to only receive delta information in the Data field on updates (info straight from oplog)
		DirectReadNs: []string{"db.users"}, // set to a slice of namespaces to read data directly from bypassing the oplog
	        DirectReadCursors:   10,            // determines the requested number of cursors to parallelCollectionScan
		Log:                 myLogger,      // pass your own logger
	})

### Direct Reads ###

If, in addition to tailing the oplog, you would like to also read entire collections you can set the DirectReadNs field
to a slice of MongoDB namespaces.  Documents from these collections will be read directly and output on the ctx.OpC channel.  

You can wait till all the collections have been fully read by using the DirectReadWg wait group on the ctx.

	go func() {
		ctx.DirectReadWg.Wait()
		fmt.Println("direct reads are done")
	}()

### Sharded Clusters ###

gtm has support for sharded MongoDB clusters.  You will want to start with a connection to the MongoDBconfig server to get the list of available shards.

    // assuming the CONFIG server for a sharded cluster is running locally on port 27018
    configSession, err = mgo.Dial("127.0.0.1:27018")
    if err != nil {
        panic(err)
    }
    // get the list of shard servers
    shardInfos := gtm.GetShards(configSession)

for each shard you will create a session and append it to a slice of sessions

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

finally you will want to start a multi context.  The multi context behaves just like a single
context except that it tails multiple shard servers and coalesces the events to a single output
channel

	multiCtx := gtm.StartMulti(shardSessions, nil)

after you have created the multi context for all the shards you can handle new shards being added
to the cluster at some later time by adding a listener. You will want to add this listener before 
you enter a loop to read events from the multi context.

	insertHandler := func(shardInfo *gtm.ShardInfo) (*mgo.Session, error) {
		log.Printf("Adding shard found at %s\n", shardInfo.GetURL())
        shardURL := shardInfo.GetURL()
        return mgo.Dial(shardURL)
	}

	multiCtx.AddShardListener(configSession, nil, insertHandler)

### Custom Unmarshalling ###

If you'd like to unmarshall MongoDB documents into your own struct instead of the document getting
unmarshalled to a generic map[string]interface{} you can use a custom unmarshal function:

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

### Workers ###

You may want to distribute event handling between a set of worker processes on different machines.
To do this you can leverage the **github.com/rwynn/gtm/consistent** package.  

Create a TOML document containing a list of all the event handlers.

	Workers = [ "Tom", "Dick", "Harry" ] 

Create a consistent filter to distribute the work between Tom, Dick, and Harry. A consistent filter
needs to acces the Data attribute of each op so it needs to be set as a Filter as opposed to a 
NamespaceFilter.
	
	name := flag.String("name", "", "the name of this worker")
	flag.Parse()
	filter, filterErr := consistent.ConsistentHashFilterFromFile(*name, "/path/to/toml")
	if filterErr != nil {
		panic(filterErr)
	}

	// there is also a method **consistent.ConsistentHashFilterFromDocument** which allows
	// you to pass a Mongo document representing the config if you would like to avoid
	// copying the same config file to multiple servers

Pass the filter into the options when calling gtm.Tail

	ctx := gtm.Start(session, &gtm.Options{Filter: filter})

If you have your multiple filters you can use the gtm utility method ChainOpFilters
	
	func ChainOpFilters(filters ...OpFilter) OpFilter

### Parallel Collection Scans ###

Gtm will attempt to enable MongoDB's parallel collection scan feature for direct reads.  Currently when using the WiredTiger
storage engine this will be disabled since WiredTiger only returns 1 cursor.  In the future, when WiredTiger is enhanced to 
support more than 1 cursor on a parallel collection scan, Gtm should enable and use multiple cursors without an upgrade.  

So, currently to get a massive speed up on direct reads one needs to use the mmapv1 storage engine which unfortunately is not the
default.

The number of cursors requested per collection is configurable via `DirectReadCursors`.  The number of cursors actually returned by
MongoDB may be less than the requested amount.  

For more information on see [Parallel Collection Scan](https://docs.mongodb.com/manual/reference/command/parallelCollectionScan/).

