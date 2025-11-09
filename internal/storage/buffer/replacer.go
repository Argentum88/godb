package buffer

import (
	"container/list"
)

type lruReplacer struct {
	list  list.List                 // Двухсвязный список для хранения FrameID. Голова - самые "свежие", хвост - самые "старые".
	nodes map[frameID]*list.Element // Хеш-таблица для быстрого O(1) доступа к узлам списка по FrameID.
}
