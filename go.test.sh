#!/usr/bin/env bash

set -e
echo "" > coverage.txt

for d in $(go list ./... \
	| grep -v vendor  \
	| grep -v "vasto/pb"); do
    go test -coverprofile=profile.out -covermode=atomic -coverpkg=github.com/chrislusf/vasto/util,github.com/chrislusf/vasto/topology,github.com/chrislusf/vasto/topology/cluster_listener,github.com/chrislusf/vasto/storage/binlog,github.com/chrislusf/vasto/storage/codec,github.com/chrislusf/vasto/storage/rocks,github.com/chrislusf/vasto/client,github.com/chrislusf/vasto/cmd/admin,github.com/chrislusf/vasto/cmd/gateway,github.com/chrislusf/vasto/cmd/master,github.com/chrislusf/vasto/cmd/shell,github.com/chrislusf/vasto/cmd/store $d
    if [ -f profile.out ]; then
        cat profile.out >> coverage.txt
        rm profile.out
    fi
done
