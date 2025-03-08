package utils

import (
	"errors"
	"fmt"
	"runtime"
)

func ErrorAt(err error) error {
	// 获取调用位置（skip=1 表示跳过当前函数的调用层）
	_, file, line, _ := runtime.Caller(1)
	return errors.Join(err, errors.New(fmt.Sprintf(" [at %s:%d]", file, line)))
}
