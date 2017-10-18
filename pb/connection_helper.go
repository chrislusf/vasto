package pb

import (
	"fmt"
	"io"

	"github.com/chrislusf/vasto/util"
	"github.com/golang/protobuf/proto"
)

func SendRequests(conn io.ReadWriter, requests *Requests) (*Responses, error) {

	var input, output []byte
	var err error

	input, err = proto.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("marshal requests: %v", err)
	}

	if err = util.WriteMessage(conn, input); err != nil {
		return nil, fmt.Errorf("write requests: %v", err)
	}

	output, err = util.ReadMessage(conn)

	if err != nil {
		return nil, fmt.Errorf("read responses: %v", err)
	}

	responses := &Responses{}
	if err = proto.Unmarshal(output, responses); err != nil {
		return nil, fmt.Errorf("unmarshal responses: %v", err)
	}

	return responses, nil

}
