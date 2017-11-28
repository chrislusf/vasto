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
	ClusterConfigFileSuffix = "-cluster.config"
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

		clusterName := name[0 : len(name)-len(ClusterConfigFileSuffix)]

		fullPath := fmt.Sprintf("%s/%s", *ss.option.Dir, name)

		log.Printf("load cluster %s from %s", clusterName, fullPath)

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

func (ss *storeServer) saveClusterConfig(clusterName string) error {

	status, ok := ss.statusInCluster[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s not found", clusterName)
	}

	txt := proto.MarshalTextString(status)

	fullPath := fmt.Sprintf("%s/%s%s", *ss.option.Dir, clusterName, ClusterConfigFileSuffix)

	log.Printf("save cluster %s to %s", clusterName, fullPath)

	return ioutil.WriteFile(fullPath, []byte(txt), 0550)

}
