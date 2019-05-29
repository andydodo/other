package util

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"time"

	yaml "gopkg.in/yaml.v2"
)

const (
	kEmptyString = ""
)

func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func Abs(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

func UnixTimestampToTime(timeStamp string) (time.Time, error) {
	if len(timeStamp) < 6 {
		return time.Unix(0, 0), &ParseError{Message: fmt.Sprintf("Parse unix timestamp failed, timestamp: %s.", timeStamp)}
	}
	seconds := timeStamp[:len(timeStamp)-6]
	microseconds := timeStamp[len(timeStamp)-6:]
	s, err := strconv.ParseInt(seconds, 10, 64)
	if err != nil {
		return time.Unix(0, 0), err
	}
	m, err := strconv.ParseInt(microseconds, 10, 64)
	if err != nil {
		return time.Unix(0, 0), err
	}
	return time.Unix(s, m*1e3), nil
}

// Min/Max call for uint64
func Min(first, second uint64) uint64 {
	if first < second {
		return first
	} else {
		return second
	}
}

func Max(first, second uint64) uint64 {
	if first < second {
		return second
	} else {
		return first
	}
}

func Md5Str(s string) string {
	r := md5.Sum([]byte(s))
	return hex.EncodeToString(r[:])
}

func Sha1Str(s string) string {
	r := sha1.Sum([]byte(s))
	return hex.EncodeToString(r[:])
}

func Md5File(fileName string) (string, error) {
	fd, err := os.Open(fileName)
	defer fd.Close()
	if err != nil {
		return kEmptyString, err
	}
	h := md5.New()
	_, err = io.Copy(h, fd)
	if err != nil {
		return kEmptyString, err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func Sha1File(fileName string) (string, error) {
	fd, err := os.Open(fileName)
	defer fd.Close()
	if err != nil {
		return kEmptyString, err
	}
	h := sha1.New()
	_, err = io.Copy(h, fd)
	if err != nil {
		return kEmptyString, err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func ParseConf(configPath string, out interface{}) (bool, error) {
	yamlFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		return false, err
	}
	err = yaml.UnmarshalStrict(yamlFile, out)

	if err != nil {
		return false, err
	}
	return true, nil
}

func NewListener(addr string) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return l, nil
}
