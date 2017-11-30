package store

import (
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/gogo/protobuf/proto"
	"io/ioutil"
	"log"
	"strings"
)

const (
	ClusterConfigFileSuffix = "cluster.config"
)

func (ss *storeServer) loadExistingClusters() error {
	files, err := ioutil.ReadDir(*ss.option.Dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		name := f.Name()
		if !strings.HasSuffix(name, ClusterConfigFileSuffix) {
			continue
		}

		clusterName := name[0: len(name)-len(ClusterConfigFileSuffix)]

		fullPath := fmt.Sprintf("%s/%s", *ss.option.Dir, name)

		log.Printf("load cluster %s config from %s", clusterName, fullPath)

		txt, err := ioutil.ReadFile(fullPath)
		if err != nil {
			log.Fatalf("read file %s: %v", fullPath, err)
		}

		status := &pb.StoreStatusInCluster{}

		if err = proto.UnmarshalText(string(txt), status); err != nil {
			log.Fatalf("parse file %s: %v", fullPath, err)
		}

		ss.statusInCluster[clusterName] = status

	}

	return nil
}

func (ss *storeServer) saveClusterConfig(status *pb.StoreStatusInCluster, clusterName string) error {

	txt := proto.MarshalTextString(status)

	fullPath := fmt.Sprintf("%s/%s/%s", *ss.option.Dir, clusterName, ClusterConfigFileSuffix)

	log.Printf("save cluster %s to %s", clusterName, fullPath)

	if err := ioutil.WriteFile(fullPath, []byte(txt), 0550); err == nil {
		ss.statusInCluster[clusterName] = status
	} else {
		log.Printf("save cluster %s to %s : %v", clusterName, fullPath, err)
	}

	return nil

}
