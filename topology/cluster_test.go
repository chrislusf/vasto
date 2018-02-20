package topology

import (
	"testing"
)

func TestClusterOperations(t *testing.T) {
	ring0 := createRing(0)
	expected := "[] size 0/0 "
	if ring0.String() != expected {
		t.Errorf("unexpected %v, %v", ring0.String(), expected)
	}

	ring3 := createRing(3)
	if ring3.String() != "[0@0,1 1@1,2 2@2,0] size 3/3 " {
		t.Errorf("unexpected %v", ring3.String())
	}

	ring3.Debug("test ")

	node, replica, found := ring3.GetNode(1, NewAccessOption(1))

	if !found || replica != 1 || node.ShardInfo.ShardId != 1 {
		t.Errorf("failed to find shard 1 replica 1")
	}

	if ring3.ExpectedSize() != 3 {
		t.Errorf("expected size: %d, %d", ring3.ExpectedSize(), 3)
	}

	if ring3.ReplicationFactor() != 2 {
		t.Errorf("expected ReplicationFactor: %d, %d", ring3.ReplicationFactor(), 2)
	}

}
