package storage


type inMemoryKV struct{
	m map[string][]byte
}

func NewInMemoryKV() Engine {
	return &inMemoryKV{
		m: make(map[string][]byte),
	}
}

func (kv *inMemoryKV) Set(key []byte, value []byte) error {
	kv.m[string(key)] = value
	return nil
}

func (kv *inMemoryKV) Get(key []byte) ([]byte, error) {
	v, ok := kv.m[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return v, nil
}
