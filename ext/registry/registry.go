package registry

import "context"

type Endpoint struct {
	Addr   string
	Weight int
	Meta   map[string]string
}

func (endpoint Endpoint) clone() Endpoint {
	c := endpoint
	if endpoint.Meta != nil {
		c.Meta = make(map[string]string, len(endpoint.Meta))
		for k, v := range endpoint.Meta {
			c.Meta[k] = v
		}
	}
	return c
}

type Registry interface {
	Register(ctx context.Context, name string, endpoint Endpoint) (dereg func(), err error)
	Watch(ctx context.Context, name string, callback func([]Endpoint)) error
}
