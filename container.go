package tasker

import "context"

type Container interface {
	//读

	GetRunningTask(ctx context.Context) ([]*Task, error)
	GetWaitingTask(ctx context.Context, limit int32) ([]*Task, error)
	GetRunningTaskCount(ctx context.Context) (int32, error)
	//GetWaitingTaskCount(ctx context.Context) (int32, error)

	//写

	AddTask(ctx context.Context, task *Task) error

	//更新

	ToSuccessStatus(ctx context.Context, task *Task) error
	ToFailedStatus(ctx context.Context, task *Task, err error) error
	ToRunningStatus(ctx context.Context, task *Task) error
	ToStoppedStatus(ctx context.Context, task *Task) error
	ToDeletedStatus(ctx context.Context, task *Task) error
	ToExportStatus(ctx context.Context, task *Task) error
	UpdateRunningTaskStatus(ctx context.Context, task *Task, status AsyncTaskStatus) error
}
