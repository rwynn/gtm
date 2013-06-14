gtm
===
gtm (go tail mongo) is a utility written in Go which tails the MongoDB oplog and sends create, update, delete events to your code.
It can be used to send emails to new users, index documents in Solr, or something else.

### Requirements ###
+ [Go](http://golang.org/doc/install)
+ [mgo](http://labix.org/mgo), the mongodb driver for Go
+ [mongodb](http://www.mongodb.org/) (setup for [replica sets](http://docs.mongodb.org/manual/tutorial/deploy-replica-set/) because otherwise there is no oplog to read from)

### Installation ###

	go get github.com/rwynn/gtm

### Usage ###
	
	package main
	
	import "labix.org/v2/mgo"
	import "labix.org/v2/mgo/bson"
	import "github.com/rwynn/gtm"
	import "fmt"

	func NewUsers(op *gtm.Op) bool {
		return op.Namespace == "users.users" && op.IsInsert()
	}

	func main() {
		// get a mgo session	
		session, err := mgo.Dial("localhost")
		if err != nil {
			panic(err)
		}
		defer session.Close()
		session.SetMode(mgo.Monotonic, true)
		
		ops, errs := gtm.Tail(session, &gtm.Options{0, nil})
		// Tail returns 2 channels - one for events and one for errors
		for {
			// loop forever receiving events	
			select {
			case err= <-errs:
				// handle errors
				fmt.Println(err)
			case op:= <-ops:
				// op will be an insert, delete or update to mongo
				// you can check which by calling op.IsInsert(), op.IsDelete(), or op.IsUpdate()
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
		// if you have an op.Timestamp and want to start receiving
		// events after that timestamp use the following
		ops, errs := gtm.Tail(session, &gtm.Options(timestamp, nil))

		// if you want to listen only for certain events on certain collections
		// pass a filter function in options
		ops, errs := gtm.Tail(session, &gtm.Options(0, NewUsers)
	}


### Advanced ###

You may want to distribute event handling between a set of worker processes on different machines.
To do this you can leverage the **github.com/rwynn/gtm/consistent** package.  

Create a json document containing a list of all the event handlers.

	{ 
		workers: ["Tom", "Dick", "Harry"] 
	}

Create a consistent filter to distribute the work between Tom, Dick, and Harry.
	
	name := flag.String("name", "", "the name of this worker")
	flag.Parse()
	filter, filterErr := consistent.ConsistentHashFilterFromFile(*name, "/path/to/json")
	if filterErr != nil {
		panic(filterErr)
	}

	// there is also a method **consistent.ConsistentHashFilterFromDocument** which allows
	// you to pass a Mongo document representing the config if you would like to avoid
	// copying the same config file to multiple servers

Pass the filter into the options when calling gtm.Tail

	ops, errs := gtm.Tail(session, &gtm.Options{0, filter})

(Optional) If you have your own filter you can use the gtm utility method ChainOpFilters
	
	func ChainOpFilters(filters ...OpFilter) OpFilter
