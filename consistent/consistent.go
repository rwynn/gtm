package consistent

import (
	"errors"
	"fmt"
	"github.com/rwynn/gtm"
	"stathat.com/c/consistent"
	"stathat.com/c/jconfig"
	"gopkg.in/mgo.v2/bson"
)

var EmptyWorkers = errors.New("config not found or workers empty")
var InvalidWorkers = errors.New("workers must be an array of string")
var WorkerMissing = errors.New("the specified worker was not found in the config")

// returns an operation filter which uses a consistent hash to determine
// if the operation will be accepted for processing. can be used to distribute work.
// name:		the name of the worker creating this filter. e.g. "Harry"
// configFile:	a file path to a json document.  the document should contain
//				an object with a property named 'workers' which is a list of
//				all the workers participating.  e.g.
//				{ "workers": ["Tom", "Dick", "Harry"] }
func ConsistentHashFilterFromFile(name string, configFile string) (gtm.OpFilter, error) {
	config := jconfig.LoadConfig(configFile)
	workers := config.GetArray("workers")
	return ConsistentHashFilter(name, workers)
}

// returns an operation filter which uses a consistent hash to determine
// if the operation will be accepted for processing. can be used to distribute work.
// name:		the name of the worker creating this filter. e.g. "Harry"
// document:	a map with a string key 'workers' which has a corresponding
//				slice of string representing the available workers
func ConsistentHashFilterFromDocument(name string, document map[string]interface{}) (gtm.OpFilter, error) {
	workers := document["workers"]
	return ConsistentHashFilter(name, workers.([]interface{}))
}

// returns an operation filter which uses a consistent hash to determine
// if the operation will be accepted for processing. can be used to distribute work.
// name:		the name of the worker creating this filter. e.g. "Harry"
// workers:		a slice of strings representing the available worker names
func ConsistentHashFilter(name string, workers []interface{}) (gtm.OpFilter, error) {
	if len(workers) == 0 {
		return nil, EmptyWorkers
	}
	found := false
	consist := consistent.New()
	for _, worker := range workers {
		next, ok := worker.(string)
		if !ok {
			return nil, InvalidWorkers
		}
		if next == name {
			found = true
		}
		consist.Add(next)
	}
	if !found {
		return nil, WorkerMissing
	}
	return func(op *gtm.Op) bool {
		var idStr string
		switch op.Id.(type) {
		case bson.ObjectId:
			idStr = op.Id.(bson.ObjectId).Hex()
		default:
			idStr = fmt.Sprintf("%v", op.Id)
		}
		who, err := consist.Get(idStr)
		if err != nil {
			return false
		} else {
			return name == who
		}
	}, nil
}
