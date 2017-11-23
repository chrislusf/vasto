package admin

import (
	"errors"
	"io"

	"github.com/chrislusf/vasto/pb"
)

type Command interface {
	Name() string
	Help() string
	Do([]string, io.Writer) error
	SetMasterCilent(masterClient pb.VastoMasterClient)
}

var commands = []Command{}

var InvalidArguments = errors.New("Invalid Arguments")
