package tasker

import "context"

type Store interface {
	Save(ctx context.Context, task *Task, data any) error
	Load(ctx context.Context, task *Task) (any, error)
	Delete(ctx context.Context, task *Task) error
}
