package tasker

import (
	"errors"
	"time"
)

type Config struct {
	Timeout     time.Duration
	TaskLimit   int32
	MaxTryTimes int32

	// 调度周期：定期发现和执行任务

	SchedulerPollInterval time.Duration

	// 状态轮训：定期获取任务执行进度

	EnableStatePoll   bool
	StatePollInterval time.Duration

	// 回调

	EnableStateCallback bool
	CallbackReceiver    CallbackReceiver

	// 完成队列

	EnableFinishedTaskList bool
}

func (c *Config) check() error {
	if !(c.EnableStatePoll || c.EnableStateCallback) {
		return errors.New("conflict DisableStatePoll is false when EnableStateCallback is true")
	}

	if c.EnableStateCallback && c.CallbackReceiver == nil {
		return errors.New("conflict EnableStateCallback is false when EnableStateCallback is true")
	}

	return nil
}
