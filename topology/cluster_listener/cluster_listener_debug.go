package cluster_listener

import "fmt"

// Debug prints out the cluster detailed information.
func (clusterListener *ClusterListener) Debug(prefix string) {
	fmt.Printf("%sevent processers:\n", prefix)
	for _, p := range clusterListener.shardEventProcessors {
		fmt.Printf("%s  * %v\n", prefix, p)
	}

	fmt.Printf("%sclusters:\n", prefix)
	clusterListener.RLock()
	for keyspaceName, cluster := range clusterListener.clusters {
		fmt.Printf("%s  keyspace: %v %v\n", prefix, keyspaceName, cluster.String())
		cluster.Debug(prefix + "    ")
	}
	clusterListener.RUnlock()
}
