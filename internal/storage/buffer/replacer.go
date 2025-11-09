package buffer

import (
	"container/list"
)

type lruReplacer struct {
	list  *list.List                // Двухсвязный список для хранения FrameID. Голова - самые "свежие", хвост - самые "старые".
	nodes map[frameID]*list.Element // Хеш-таблица для быстрого O(1) доступа к узлам списка по FrameID.
}

func NewLRUReplacer() *lruReplacer {
	return &lruReplacer{
		list: list.New(),
		nodes: make(map[frameID]*list.Element),
	}
}

func (r *lruReplacer) Pin(frameID frameID) {
	el, ok := r.nodes[frameID]
	if !ok {
		return
	}

	delete(r.nodes, frameID)
	r.list.Remove(el)
}

func (r *lruReplacer) Unpin(frameID frameID) {
	el := r.list.PushFront(frameID)
	r.nodes[frameID] = el
}

func (r *lruReplacer) Evict() (frameID, bool) {
	el := r.list.Back()
	if el == nil {
		return 0, false
	}

	frameID, ok := el.Value.(frameID)
	if !ok {
		panic("failed to assert frameID type")
	}

	r.list.Remove(el)
	delete(r.nodes, frameID)

	return frameID, true
}
