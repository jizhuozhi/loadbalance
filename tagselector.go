package loadbalance

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/loadbalance"
)

type TagFunc func(ctx context.Context, request interface{}) string

type tagSelectorBalancer struct {
	tag         string
	tagFunc     TagFunc
	next        loadbalance.Loadbalancer
	pickerCache sync.Map
	sfg         singleflight.Group
}

func NewTagSelector(tag string, f TagFunc, next loadbalance.Loadbalancer) loadbalance.Loadbalancer {
	return &tagSelectorBalancer{
		tag:     tag,
		tagFunc: f,
		next:    next,
	}
}

func (b *tagSelectorBalancer) GetPicker(e discovery.Result) loadbalance.Picker {
	if !e.Cacheable {
		p := b.createPicker(e)
		return p
	}

	p, ok := b.pickerCache.Load(e.CacheKey)
	if !ok {
		p, _, _ = b.sfg.Do(e.CacheKey, func() (interface{}, error) {
			pp := b.createPicker(e)
			b.pickerCache.Store(e.CacheKey, pp)
			return pp, nil
		})
	}
	return p.(loadbalance.Picker)
}

func (b *tagSelectorBalancer) Rebalance(change discovery.Change) {
	if !change.Result.Cacheable {
		return
	}
	b.pickerCache.Store(change.Result.CacheKey, b.createPicker(change.Result))
}

func (b *tagSelectorBalancer) Delete(change discovery.Change) {
	if !change.Result.Cacheable {
		return
	}
	b.pickerCache.Delete(change.Result.CacheKey)
}

func (b *tagSelectorBalancer) createPicker(e discovery.Result) loadbalance.Picker {
	tagInstances := make(map[string][]discovery.Instance)
	for _, instance := range e.Instances {
		if t, ok := instance.Tag(b.tag); ok {
			tagInstances[t] = append(tagInstances[t], instance)
		} else {
			tagInstances[""] = append(tagInstances[""], instance)
		}
	}

	tagPickers := make(map[string]loadbalance.Picker, len(tagInstances))
	for t, instances := range tagInstances {
		// a projection of raw discovery.Result has same cache option
		p := b.next.GetPicker(discovery.Result{
			Cacheable: e.Cacheable,
			CacheKey:  e.CacheKey,
			Instances: instances,
		})
		tagPickers[t] = p
	}

	return &tagSelectorPicker{
		tagFunc:    b.tagFunc,
		tagPickers: tagPickers,
	}
}

func (b *tagSelectorBalancer) Name() string {
	return "tagselector_" + b.next.Name()
}

type tagSelectorPicker struct {
	tagFunc    TagFunc
	tagPickers map[string]loadbalance.Picker
}

func (p *tagSelectorPicker) Next(ctx context.Context, request interface{}) discovery.Instance {
	t := p.tagFunc(ctx, request)
	if pp, ok := p.tagPickers[t]; ok {
		return pp.Next(ctx, request)
	}
	return nil
}
