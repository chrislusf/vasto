package store

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/chrislusf/vasto/pb"
	"github.com/golang/protobuf/proto"
)

const (
	ClusterConfigFile = "cluster.config"
)

func (ss *storeServer) listExistingClusters() error {
	files, err := ioutil.ReadDir(*ss.option.Dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		keyspaceName := f.Name()
		fullPath := fmt.Sprintf("%s/%s/%s", *ss.option.Dir, keyspaceName, ClusterConfigFile)
		txt, err := ioutil.ReadFile(fullPath)
		if err != nil {
			log.Printf("read file %s: %v", fullPath, err)
			continue
		}
		log.Printf("load cluster %s config from %s", keyspaceName, fullPath)

		status := &pb.StoreStatusInCluster{}

		if err = proto.UnmarshalText(string(txt), status); err != nil {
			log.Printf("parse file %s: %v", fullPath, err)
			continue
		}

		ss.statusInCluster[keyspaceName] = status

	}

	return nil
}

func (ss *storeServer) saveClusterConfig(status *pb.StoreStatusInCluster, keyspaceName string) error {

	txt := proto.MarshalTextString(status)

	fullPath := fmt.Sprintf("%s/%s/%s", *ss.option.Dir, keyspaceName, ClusterConfigFile)

	log.Printf("save cluster %s to %s", keyspaceName, fullPath)

	if err := ioutil.WriteFile(fullPath, []byte(txt), 0640); err == nil {
		ss.statusInCluster[keyspaceName] = status
	} else {
		log.Printf("save cluster %s to %s : %v", keyspaceName, fullPath, err)
	}

	return nil

}
