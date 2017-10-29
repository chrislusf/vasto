package change_log

import (
	"io"
	"time"
	"unsafe"
)

var (
	_ = unsafe.Sizeof(0)
	_ = io.ReadFull
	_ = time.Now()
)

type LogEntry struct {
	PartitionHash      uint64
	UpdatedNanoSeconds uint64
	TtlSecond          uint32
	IsDelete           bool
	Crc                uint32
	Key                []byte
	Value              []byte
}

func (d *LogEntry) Size() (s uint64) {

	{
		l := uint64(len(d.Key))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.Value))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	s += 25
	return
}
func (d *LogEntry) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{

		buf[0+0] = byte(d.PartitionHash >> 0)

		buf[1+0] = byte(d.PartitionHash >> 8)

		buf[2+0] = byte(d.PartitionHash >> 16)

		buf[3+0] = byte(d.PartitionHash >> 24)

		buf[4+0] = byte(d.PartitionHash >> 32)

		buf[5+0] = byte(d.PartitionHash >> 40)

		buf[6+0] = byte(d.PartitionHash >> 48)

		buf[7+0] = byte(d.PartitionHash >> 56)

	}
	{

		buf[0+8] = byte(d.UpdatedNanoSeconds >> 0)

		buf[1+8] = byte(d.UpdatedNanoSeconds >> 8)

		buf[2+8] = byte(d.UpdatedNanoSeconds >> 16)

		buf[3+8] = byte(d.UpdatedNanoSeconds >> 24)

		buf[4+8] = byte(d.UpdatedNanoSeconds >> 32)

		buf[5+8] = byte(d.UpdatedNanoSeconds >> 40)

		buf[6+8] = byte(d.UpdatedNanoSeconds >> 48)

		buf[7+8] = byte(d.UpdatedNanoSeconds >> 56)

	}
	{

		buf[0+16] = byte(d.TtlSecond >> 0)

		buf[1+16] = byte(d.TtlSecond >> 8)

		buf[2+16] = byte(d.TtlSecond >> 16)

		buf[3+16] = byte(d.TtlSecond >> 24)

	}
	{
		if d.IsDelete {
			buf[20] = 1
		} else {
			buf[20] = 0
		}
	}
	{

		buf[0+21] = byte(d.Crc >> 0)

		buf[1+21] = byte(d.Crc >> 8)

		buf[2+21] = byte(d.Crc >> 16)

		buf[3+21] = byte(d.Crc >> 24)

	}
	{
		l := uint64(len(d.Key))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+25] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+25] = byte(t)
			i++

		}
		copy(buf[i+25:], d.Key)
		i += l
	}
	{
		l := uint64(len(d.Value))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+25] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+25] = byte(t)
			i++

		}
		copy(buf[i+25:], d.Value)
		i += l
	}
	return buf[:i+25], nil
}

func (d *LogEntry) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{

		d.PartitionHash = 0 | (uint64(buf[i+0+0]) << 0) | (uint64(buf[i+1+0]) << 8) | (uint64(buf[i+2+0]) << 16) | (uint64(buf[i+3+0]) << 24) | (uint64(buf[i+4+0]) << 32) | (uint64(buf[i+5+0]) << 40) | (uint64(buf[i+6+0]) << 48) | (uint64(buf[i+7+0]) << 56)

	}
	{

		d.UpdatedNanoSeconds = 0 | (uint64(buf[i+0+8]) << 0) | (uint64(buf[i+1+8]) << 8) | (uint64(buf[i+2+8]) << 16) | (uint64(buf[i+3+8]) << 24) | (uint64(buf[i+4+8]) << 32) | (uint64(buf[i+5+8]) << 40) | (uint64(buf[i+6+8]) << 48) | (uint64(buf[i+7+8]) << 56)

	}
	{

		d.TtlSecond = 0 | (uint32(buf[i+0+16]) << 0) | (uint32(buf[i+1+16]) << 8) | (uint32(buf[i+2+16]) << 16) | (uint32(buf[i+3+16]) << 24)

	}
	{
		d.IsDelete = buf[i+20] == 1
	}
	{

		d.Crc = 0 | (uint32(buf[i+0+21]) << 0) | (uint32(buf[i+1+21]) << 8) | (uint32(buf[i+2+21]) << 16) | (uint32(buf[i+3+21]) << 24)

	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+25] & 0x7F)
			for buf[i+25]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+25]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Key)) >= l {
			d.Key = d.Key[:l]
		} else {
			d.Key = make([]byte, l)
		}
		copy(d.Key, buf[i+25:])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+25] & 0x7F)
			for buf[i+25]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+25]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Value)) >= l {
			d.Value = d.Value[:l]
		} else {
			d.Value = make([]byte, l)
		}
		copy(d.Value, buf[i+25:])
		i += l
	}
	return i + 25, nil
}
