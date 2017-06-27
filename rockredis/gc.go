package rockredis

import (
	"fmt"
	"sync"
)

/*
	used to collect the redundant data generated at run time
*/

type GCComponent interface {
	Name() string
	Start()
	Stop()
	Stats() (interface{}, error)
}

type GC struct {
	sync.Mutex
	wg         sync.WaitGroup
	components map[string]GCComponent
}

func NewGC() *GC {
	return &GC{
		components: make(map[string]GCComponent),
	}
}

func (gc *GC) AddComponent(component GCComponent) error {
	defer gc.Unlock()
	gc.Lock()
	name := component.Name()
	if _, ok := gc.components[name]; ok {
		return fmt.Errorf("gc component named:%s already exists", name)
	} else {
		gc.components[name] = component
	}
	return nil
}

func (gc *GC) Start() {
	for _, component := range gc.components {
		gc.wg.Add(1)
		go func(c GCComponent) {
			c.Start()
			gc.wg.Done()
		}(component)
	}
}

func (gc *GC) ComponentStop(name string) {
	gc.Lock()
	if component, ok := gc.components[name]; ok {
		component.Stop()
	}
	gc.Unlock()
}

func (gc *GC) StopAll() {
	gc.Lock()
	for _, component := range gc.components {
		component.Stop()
	}
	gc.Unlock()
	gc.wg.Wait()
}

func (gc *GC) GetStats(name string) (interface{}, error) {
	defer gc.Unlock()
	gc.Lock()
	if c, ok := gc.components[name]; !ok {
		return nil, fmt.Errorf("gc component named:%s do not exist", name)
	} else {
		return c.Stats()
	}
}
