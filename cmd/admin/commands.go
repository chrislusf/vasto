package admin

import (
	"errors"
	"io"

	"github.com/chrislusf/vasto/pb"
)

type command interface {
	Name() string
	Help() string
	Do([]string, io.Writer) error
	SetMasterCilent(masterClient pb.VastoMasterClient)
}

var commands = []command{}

var invalidArguments = errors.New("invalid arguments")
