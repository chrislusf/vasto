package store

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/chrislusf/vasto/pb"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
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

		status := &pb.LocalShardsInCluster{}

		if err = proto.UnmarshalText(string(txt), status); err != nil {
			log.Printf("parse file %s: %v", fullPath, err)
			continue
		}

		ss.statusInCluster[keyspaceName] = status

	}

	return nil
}

func (ss *storeServer) saveClusterConfig(status *pb.LocalShardsInCluster, keyspaceName string) error {

	ss.statusInClusterLock.Lock()
	defer ss.statusInClusterLock.Unlock()

	txt := proto.MarshalTextString(status)

	fullPath := fmt.Sprintf("%s/%s/%s", *ss.option.Dir, keyspaceName, ClusterConfigFile)

	log.Printf("save cluster %s to %s", keyspaceName, fullPath)

	if err := ioutil.WriteFile(fullPath, []byte(txt), 0640); err == nil {
		ss.statusInCluster[keyspaceName] = status
	} else {
		log.Printf("%+v", errors.WithStack(err))
		return errors.Errorf("save cluster %s to %s : %v", keyspaceName, fullPath, err)
	}

	return nil

}

func (ss *storeServer) getServerStatusInCluster(keyspace string) (statusInCluster *pb.LocalShardsInCluster, found bool) {

	ss.statusInClusterLock.RLock()
	defer ss.statusInClusterLock.RUnlock()

	statusInCluster, found = ss.statusInCluster[keyspace]

	return

}

func (ss *storeServer) deleteServerStatusInCluster(keyspace string) {

	ss.statusInClusterLock.RLock()
	defer ss.statusInClusterLock.RUnlock()

	delete(ss.statusInCluster, keyspace)

	return

}

func (ss *storeServer) getOrCreateServerStatusInCluster(keyspace string, serverId, clusterSize, replicationFactor int) (*pb.LocalShardsInCluster) {

	ss.statusInClusterLock.Lock()
	defer ss.statusInClusterLock.Unlock()

	statusInCluster, found := ss.statusInCluster[keyspace]
	if !found {
		statusInCluster = &pb.LocalShardsInCluster{
			Id:                uint32(serverId),
			ShardMap:          make(map[uint32]*pb.ShardInfo),
			ClusterSize:       uint32(clusterSize),
			ReplicationFactor: uint32(replicationFactor),
		}
	}

	return statusInCluster

}
