package pb

import (
	"fmt"
	"io"

	"github.com/chrislusf/vasto/util"
	"github.com/golang/protobuf/proto"
)

func SendRequest(conn io.ReadWriter, request *Requests) (*Responses, error) {

	var input, output []byte
	var err error

	input, err = proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %v", err)
	}

	if err = util.WriteMessage(conn, input); err != nil {
		return nil, fmt.Errorf("write request: %v", err)
	}

	output, err = util.ReadMessage(conn)

	if err != nil {
		return nil, fmt.Errorf("read response: %v", err)
	}

	responses := &Responses{}
	if err = proto.Unmarshal(output, responses); err != nil {
		return nil, fmt.Errorf("unmarshal response: %v", err)
	}

	return responses, nil

}
