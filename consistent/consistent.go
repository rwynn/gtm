package consistent

import (
	"github.com/rwynn/gtm"
	"github.com/stathat/consistent"
	"github.com/stathat/jconfig"
	"labix.org/v2/mgo/bson"
	"errors"
	"fmt"
)

var EmptyWorkers = errors.New("Config not found or workers empty")

// returns an operation filter which uses a consistent hash to determine
// if the operation will be accepted. can be used to distribute work. 
// name:		the name of the worker creating this filter. e.g. "Harry"
// configFile:	a file path to a json document.  the document should contain
//				an object with a property named 'workers' which is a list of
//				all the workers participating.  e.g. 
//				{ "workers": ["Tom", "Dick", "Harry"] }
func ConsistentHashFilter(name string, configFile string) (gtm.OpFilter, error) {
	config := jconfig.LoadConfig(configFile)
	workers := config.GetArray("workers")
	if len(workers) == 0 {
		return nil, EmptyWorkers
	}
	consist := consistent.New()
	for _, worker := range workers {
		consist.Add(worker.(string))
	}
	return func(op *gtm.Op) bool {
		var idStr string
		switch op.Id.(type) {
			case bson.ObjectId:
				idStr = op.Id.(bson.ObjectId).Hex()
			default:
				idStr = fmt.Sprintf("%v", op)
		}
		who, err := consist.Get(idStr)
		if err != nil {
			return false
		} else {
			return name == who
		}
	}, nil
}
