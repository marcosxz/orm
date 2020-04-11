package orm

import (
	"bytes"
	"strconv"
	"strings"
	"unsafe"
)

type XOrmStringArray []string

func (sa *XOrmStringArray) FromDB(bs []byte) error {
	if len(bs) == 0 {
		return nil
	}
	ss := strings.Split(*(*string)(unsafe.Pointer(&bs)), ",")
	for _, s := range ss {
		if len(s) > 0 {
			*sa = append(*sa, s)
		}
	}
	return nil
}

func (sa *XOrmStringArray) ToDB() ([]byte, error) {
	if sa == nil || *sa == nil || len(*sa) == 0 {
		return nil, nil
	}
	return []byte(strings.Join(*sa, ",")), nil
}

type XOrmIntegerArray []int

func (ia *XOrmIntegerArray) FromDB(bs []byte) error {
	if len(bs) == 0 {
		return nil
	}
	ss := strings.Split(string(bs), ",")
	for _, s := range ss {
		if len(s) > 0 {
			if val, err := strconv.Atoi(s); err != nil {
				return err
			} else {
				*ia = append(*ia, val)
			}
		}
	}
	return nil
}

func (ia *XOrmIntegerArray) ToDB() ([]byte, error) {
	if ia == nil || *ia == nil || len(*ia) == 0 {
		return nil, nil
	}
	buf := new(bytes.Buffer)
	for _, i := range *ia {
		buf.WriteString(strconv.Itoa(i))
		buf.WriteString(",")
	}
	array := buf.String()
	array = array[:len(array)-1]
	return []byte(array), nil
}
