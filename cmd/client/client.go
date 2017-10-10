package client

type ClientOption struct {
	Master *string
}

type VastoClient struct {
	option *ClientOption
}

func New(option *ClientOption) *VastoClient {
	c := &VastoClient{
		option: option,
	}
	return c
}

func connectToMaster() {

}
