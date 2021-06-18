package metric

// slow write logs
// large keys
// large collections
// large deletion
// large read/scan
// use (LRU) for topn hot keys, large keys and large collections

import (
	"sort"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru"
	"github.com/twmb/murmur3"
)

type HKeyInfo struct {
	//Value      []byte
	Cnt int32
	//InitTime time.Time
}

func (hki *HKeyInfo) Inc() {
	atomic.AddInt32(&hki.Cnt, 1)
}

const (
	defaultTopnBucketSize = 2
	maxTopnInBucket       = 16
)

var TopnHotKeys = NewTopNHot()

type topNBucket struct {
	hotWriteKeys *lru.ARCCache
	sampleCnt    int64
}

func newTopNBucket() *topNBucket {
	l, err := lru.NewARC(maxTopnInBucket)
	if err != nil {
		panic(err)
	}
	return &topNBucket{
		hotWriteKeys: l,
	}
}

func handleTopnHit(hotKeys *lru.ARCCache, k []byte) *HKeyInfo {
	item, ok := hotKeys.Get(string(k))
	var hki *HKeyInfo
	if ok {
		hki = item.(*HKeyInfo)
		hki.Inc()
	} else {
		// if concurrent add, just ignore will be ok
		hki = &HKeyInfo{
			Cnt: 1,
			//InitTime: time.Now(),
		}
		hotKeys.Add(string(k), hki)
	}
	return hki
}

func (b *topNBucket) write(k []byte) {
	c := atomic.AddInt64(&b.sampleCnt, 1)
	if c%3 != 0 {
		return
	}
	handleTopnHit(b.hotWriteKeys, k)
}

func (b *topNBucket) read(k []byte) {
	// currently read no need
	return
}

func (b *topNBucket) Clear() {
	b.hotWriteKeys.Purge()
}

func (b *topNBucket) Keys() []interface{} {
	return b.hotWriteKeys.Keys()
}

func (b *topNBucket) Peek(key interface{}) *HKeyInfo {
	v, ok := b.hotWriteKeys.Peek(key)
	if !ok {
		return nil
	}
	item, ok := v.(*HKeyInfo)
	if !ok {
		return nil
	}
	return item
}

type TopNHot struct {
	hotKeys [defaultTopnBucketSize]*topNBucket
	enabled int32
}

func NewTopNHot() *TopNHot {
	top := &TopNHot{
		enabled: 1,
	}
	for i := 0; i < len(top.hotKeys); i++ {
		top.hotKeys[i] = newTopNBucket()
	}
	return top
}

// Clear will clear all history lru data. Period reset can make sure
// some new data can be refreshed to lru
func (tnh *TopNHot) Clear() {
	for _, b := range tnh.hotKeys {
		b.Clear()
	}
}

func (tnh *TopNHot) isEnabled() bool {
	return atomic.LoadInt32(&tnh.enabled) > 0
}

func (tnh *TopNHot) Enable(on bool) {
	if on {
		atomic.StoreInt32(&tnh.enabled, 1)
	} else {
		atomic.StoreInt32(&tnh.enabled, 0)
	}
}

func (tnh *TopNHot) getBucket(k []byte) (*topNBucket, uint64) {
	hk := murmur3.Sum64(k)
	bi := hk % uint64(len(tnh.hotKeys))
	b := tnh.hotKeys[bi]
	return b, bi
}

func (tnh *TopNHot) HitWrite(k []byte) {
	if !tnh.isEnabled() {
		return
	}
	if len(k) == 0 {
		return
	}
	b, _ := tnh.getBucket(k)
	b.write(k)
}

type TopNInfo struct {
	Key string
	Cnt int32
}

type topnList []TopNInfo

func (t topnList) Len() int {
	return len(t)
}
func (t topnList) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
func (t topnList) Less(i, j int) bool {
	if t[i].Cnt == t[j].Cnt {
		return t[i].Key < t[j].Key
	}
	return t[i].Cnt < t[j].Cnt
}

func (tnh *TopNHot) GetTopNWrites() []TopNInfo {
	if !tnh.isEnabled() {
		return nil
	}
	hks := make(topnList, 0, len(tnh.hotKeys))
	for _, b := range tnh.hotKeys {
		keys := b.Keys()
		for _, key := range keys {
			v := b.Peek(key)
			if v == nil {
				continue
			}
			hks = append(hks, TopNInfo{Key: key.(string), Cnt: atomic.LoadInt32(&v.Cnt)})
		}
	}
	sort.Sort(hks)
	return hks
}
