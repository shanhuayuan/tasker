package tasker

import "context"

type Actuator interface {
	Init(ctx context.Context, task *Task) error
	Start(ctx context.Context, task *Task) (ignore bool, err error)
	Stop(ctx context.Context, task *Task) error

	GetOutput(ctx context.Context, task *Task) (any, error)
	GetAsyncTaskStatus(ctx context.Context, tasks []*Task) ([]AsyncTaskStatus, error)
}
