package topology

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBootstrapPeersWhenShrinkingSmall(t *testing.T) {
	/*

		curr server id  : 0 1 2 3 4 5 6
		replica shard 0 : 0 1 2 3 4 5 6
		replica shard 1 : 6 0 1 2 3 4 5
		replica shard 2 : 5 6 0 1 2 3 4

		next server id  : 0 1 2 3 4 5
		replica shard 0 : 0 1 2 3 4 5
		replica shard 1 : 5 0 1 2 3 4
		replica shard 2 : 4 5 0 1 2 3

	*/

	plan := BootstrapPlanWithTopoChange(&BootstrapRequest{5, 4, 7, 6, 3})
	assert.Equal(t, 1, len(plan.BootstrapSource))
	assert.Equal(t, 6, plan.BootstrapSource[0].ServerId)

	plan = BootstrapPlanWithTopoChange(&BootstrapRequest{0, 4, 7, 6, 3})
	assert.Equal(t, 3, len(plan.BootstrapSource))

	plan = BootstrapPlanWithTopoChange(&BootstrapRequest{0, 5, 7, 6, 3})
	assert.Equal(t, 1, len(plan.BootstrapSource))
	assert.Equal(t, 6, plan.BootstrapSource[0].ServerId)

}

func TestBootstrapPeersWhenShrinkingBig(t *testing.T) {
	/*

		curr server id  : 0 1 2 3 4 5 6 7 8 9
		replica shard 0 : 0 1 2 3 4 5 6 7 8 9
		replica shard 1 : 9 0 1 2 3 4 5 6 7 8
		replica shard 2 : 8 9 0 1 2 3 4 5 6 7

		next server id  : 0 1 2 3 4 5
		replica shard 0 : 0 1 2 3 4 5
		replica shard 1 : 5 0 1 2 3 4
		replica shard 2 : 4 5 0 1 2 3

	*/

	plan := BootstrapPlanWithTopoChange(&BootstrapRequest{5, 4, 10, 6, 3})
	assert.Equal(t, 4, len(plan.BootstrapSource))
	assert.Equal(t, 6, plan.BootstrapSource[0].ServerId)
	assert.Equal(t, 7, plan.BootstrapSource[1].ServerId)
	assert.Equal(t, 8, plan.BootstrapSource[2].ServerId)
	assert.Equal(t, 9, plan.BootstrapSource[3].ServerId)

	plan = BootstrapPlanWithTopoChange(&BootstrapRequest{0, 4, 10, 6, 3})
	assert.Equal(t, 3, len(plan.BootstrapSource))

	plan = BootstrapPlanWithTopoChange(&BootstrapRequest{0, 8, 10, 6, 3})
	assert.Equal(t, 0, len(plan.BootstrapSource))

}

func TestBootstrapPeersWhenGrowingSmall(t *testing.T) {
	/*

		curr server id  : 0 1 2 3 4 5
		replica shard 0 : 0 1 2 3 4 5
		replica shard 1 : 5 0 1 2 3 4
		replica shard 2 : 4 5 0 1 2 3

		next server id  : 0 1 2 3 4 5 6
		replica shard 0 : 0 1 2 3 4 5 6
		replica shard 1 : 6 0 1 2 3 4 5
		replica shard 2 : 5 6 0 1 2 3 4

	*/

	plan := BootstrapPlanWithTopoChange(&BootstrapRequest{6, 6, 6, 7, 3})
	assert.Equal(t, 6, len(plan.BootstrapSource))
	assert.Equal(t, 0, plan.BootstrapSource[0].ServerId)
	assert.Equal(t, 1, plan.BootstrapSource[1].ServerId)
	assert.Equal(t, 2, plan.BootstrapSource[2].ServerId)
	assert.Equal(t, 3, plan.BootstrapSource[3].ServerId)
	assert.Equal(t, 4, plan.BootstrapSource[4].ServerId)
	assert.Equal(t, 5, plan.BootstrapSource[5].ServerId)

	plan = BootstrapPlanWithTopoChange(&BootstrapRequest{0, 6, 6, 7, 3})
	assert.Equal(t, 6, len(plan.BootstrapSource))
	assert.Equal(t, 0, plan.BootstrapSource[0].ServerId)
	assert.Equal(t, 1, plan.BootstrapSource[1].ServerId)
	assert.Equal(t, 2, plan.BootstrapSource[2].ServerId)
	assert.Equal(t, 3, plan.BootstrapSource[3].ServerId)
	assert.Equal(t, 4, plan.BootstrapSource[4].ServerId)
	assert.Equal(t, 5, plan.BootstrapSource[5].ServerId)

	plan = BootstrapPlanWithTopoChange(&BootstrapRequest{0, 5, 6, 7, 3})
	assert.Equal(t, 0, len(plan.BootstrapSource))

	plan = BootstrapPlanWithTopoChange(&BootstrapRequest{3, 2, 6, 7, 3})
	assert.Equal(t, 0, len(plan.BootstrapSource))

	plan = BootstrapPlanWithTopoChange(&BootstrapRequest{6, 4, 6, 7, 3})
	assert.Equal(t, 3, len(plan.BootstrapSource))
	assert.Equal(t, 4, plan.BootstrapSource[0].ServerId)

}

func TestBootstrapPeersWhenGrowingBig(t *testing.T) {
	/*

		curr server id  : 0 1 2 3 4 5
		replica shard 0 : 0 1 2 3 4 5
		replica shard 1 : 5 0 1 2 3 4
		replica shard 2 : 4 5 0 1 2 3

		next server id  : 0 1 2 3 4 5 6 7 8 9
		replica shard 0 : 0 1 2 3 4 5 6 7 8 9
		replica shard 1 : 9 0 1 2 3 4 5 6 7 8
		replica shard 2 : 8 9 0 1 2 3 4 5 6 7
	*/

	plan := BootstrapPlanWithTopoChange(&BootstrapRequest{6, 6, 6, 10, 3})
	assert.Equal(t, 6, len(plan.BootstrapSource))
	assert.Equal(t, 0, plan.BootstrapSource[0].ServerId)
	assert.Equal(t, 1, plan.BootstrapSource[1].ServerId)
	assert.Equal(t, 2, plan.BootstrapSource[2].ServerId)
	assert.Equal(t, 3, plan.BootstrapSource[3].ServerId)
	assert.Equal(t, 4, plan.BootstrapSource[4].ServerId)
	assert.Equal(t, 5, plan.BootstrapSource[5].ServerId)

	plan = BootstrapPlanWithTopoChange(&BootstrapRequest{0, 9, 6, 10, 3})
	assert.Equal(t, 6, len(plan.BootstrapSource))
	assert.Equal(t, 0, plan.BootstrapSource[0].ServerId)
	assert.Equal(t, 1, plan.BootstrapSource[1].ServerId)
	assert.Equal(t, 2, plan.BootstrapSource[2].ServerId)
	assert.Equal(t, 3, plan.BootstrapSource[3].ServerId)
	assert.Equal(t, 4, plan.BootstrapSource[4].ServerId)
	assert.Equal(t, 5, plan.BootstrapSource[5].ServerId)

	plan = BootstrapPlanWithTopoChange(&BootstrapRequest{7, 5, 6, 10, 3})
	assert.Equal(t, 3, len(plan.BootstrapSource))
	assert.Equal(t, 5, plan.BootstrapSource[0].ServerId)

	plan = BootstrapPlanWithTopoChange(&BootstrapRequest{3, 2, 6, 10, 3})
	assert.Equal(t, 0, len(plan.BootstrapSource))

}
