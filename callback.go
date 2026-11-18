package tasker

import "context"

type CallbackReceiver interface {
	GetCallbackChannel(ctx context.Context) chan *Task
}
