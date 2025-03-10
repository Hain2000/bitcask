package redis

type hashInternalKey struct {
	key   []byte
	filed []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.filed)+8)
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// filed
	copy(buf[index:], hk.filed)

	return buf
}
