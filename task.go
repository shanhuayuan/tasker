package tasker

import "time"

type Status int32

const (
	TaskStatusInvalid   Status = 0
	TaskStatusUnstart   Status = 1
	TaskStatusWaiting   Status = 2
	TaskStatusRunning   Status = 3
	TaskStatusSuccess   Status = 4
	TaskStatusFailed    Status = 5
	TaskStatusStopped   Status = 6
	TaskStatusDelete    Status = 7
	TaskStatusExporting Status = 8
)

type Task struct {
	Id       string
	Priority int
	Item     any
	Status   Status

	StartedAt time.Time
	EndedAt   time.Time
	Error     error
	TryTimes  int32
}

type AsyncTaskStatus struct {
	Status   Status
	Error    error
	Progress any
}
