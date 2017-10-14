package rocks

func (d *rocks) Put(key []byte, msg []byte) error {
	return d.db.Put(d.wo, key, msg)
}
