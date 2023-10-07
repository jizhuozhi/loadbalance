package loadbalance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/loadbalance"
)

type mockBalancer struct {
}

type mockPicker struct {
	result discovery.Result
}

func (m *mockBalancer) GetPicker(result discovery.Result) loadbalance.Picker {
	return &mockPicker{result: result}
}

func (m *mockPicker) Next(ctx context.Context, request interface{}) discovery.Instance {
	return m.result.Instances[0]
}

func (m *mockBalancer) Name() string {
	return "mock"
}

func TestTagSelectorBalancer_Name(t *testing.T) {
	lb := NewTagSelector("foo", func(ctx context.Context, request interface{}) string {
		return ""
	}, &mockBalancer{})
	assert.Equal(t, "tagselector_mock", lb.Name())
}

func TestTagSelectorBalancer_GetPicker(t *testing.T) {
	testcases := []struct {
		cacheable    bool
		cacheKey     string
		instances    []discovery.Instance
		tagInstances map[string][]discovery.Instance
	}{
		{}, // nil
		{
			cacheable: true,
			cacheKey:  "one instance",
			instances: []discovery.Instance{discovery.NewInstance("tcp", "addr1", 10, map[string]string{"foo": "bar"})},
			tagInstances: map[string][]discovery.Instance{
				"bar": {discovery.NewInstance("tcp", "addr1", 10, map[string]string{"foo": "bar"})},
			},
		},
		{
			cacheable: false,
			cacheKey:  "multi instances",
			instances: []discovery.Instance{
				discovery.NewInstance("tcp", "addr1", 10, map[string]string{"foo": "bar1"}),
				discovery.NewInstance("tcp", "addr2", 20, map[string]string{"foo": "bar2"}),
				discovery.NewInstance("tcp", "addr3", 30, map[string]string{"foo": "bar3"}),
				discovery.NewInstance("tcp", "addr4", 30, map[string]string{"foo": ""}),
				discovery.NewInstance("tcp", "addr5", 30, nil),
			},
			tagInstances: map[string][]discovery.Instance{
				"bar1": {discovery.NewInstance("tcp", "addr1", 10, map[string]string{"foo": "bar1"})},
				"bar2": {discovery.NewInstance("tcp", "addr2", 20, map[string]string{"foo": "bar2"})},
				"bar3": {discovery.NewInstance("tcp", "addr3", 30, map[string]string{"foo": "bar3"})},
				"": {
					discovery.NewInstance("tcp", "addr4", 30, map[string]string{"foo": ""}),
					discovery.NewInstance("tcp", "addr5", 30, nil),
				},
			},
		},
	}

	lb := NewTagSelector("foo", func(ctx context.Context, request interface{}) string {
		return ""
	}, &mockBalancer{})

	for _, tt := range testcases {
		p := lb.GetPicker(discovery.Result{
			Cacheable: tt.cacheable,
			CacheKey:  tt.cacheKey,
			Instances: tt.instances,
		})
		assert.NotNil(t, p)
		assert.IsType(t, &tagSelectorPicker{}, p)

		pp := p.(*tagSelectorPicker)
		assert.Len(t, pp.tagPickers, len(tt.tagInstances))
		for k, v := range tt.tagInstances {
			p := pp.tagPickers[k]
			assert.IsType(t, &mockPicker{}, p)

			pp := p.(*mockPicker)
			assert.Equal(t, tt.cacheable, pp.result.Cacheable)
			assert.Equal(t, tt.cacheKey, pp.result.CacheKey)
			assert.EqualValues(t, v, pp.result.Instances)
		}
	}

	// once
	p1 := lb.GetPicker(discovery.Result{
		Cacheable: true,
		CacheKey:  "cached",
		Instances: nil,
	})

	p2 := lb.GetPicker(discovery.Result{
		Cacheable: true,
		CacheKey:  "cached",
		Instances: nil,
	})

	assert.Same(t, p1, p2)
}

func TestTagSelectorPicker_Next(t *testing.T) {
	lb := NewTagSelector("foo", func(ctx context.Context, request interface{}) string {
		return request.(string)
	}, &mockBalancer{})

	p := lb.GetPicker(discovery.Result{
		Instances: []discovery.Instance{
			discovery.NewInstance("tcp", "addr1", 10, map[string]string{"foo": "bar1"}),
			discovery.NewInstance("tcp", "addr2", 20, map[string]string{"foo": "bar2"}),
			discovery.NewInstance("tcp", "addr3", 30, map[string]string{"foo": "bar3"}),
			discovery.NewInstance("tcp", "addr4", 40, map[string]string{"foo": ""}),
		},
	})

	testcases := []struct {
		req    string
		expect discovery.Instance
	}{
		{"bar1", discovery.NewInstance("tcp", "addr1", 10, map[string]string{"foo": "bar1"})},
		{"bar2", discovery.NewInstance("tcp", "addr2", 20, map[string]string{"foo": "bar2"})},
		{"bar3", discovery.NewInstance("tcp", "addr3", 30, map[string]string{"foo": "bar3"})},
		{"", discovery.NewInstance("tcp", "addr4", 40, map[string]string{"foo": ""})},
		{"missed", nil},
	}

	for _, tt := range testcases {
		instance := p.Next(context.Background(), tt.req)
		assert.Equal(t, tt.expect, instance)
	}
}
