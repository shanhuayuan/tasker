package tasker

import (
	"context"
	"errors"
	"sync"
	"time"

	stlextension "github.com/memory-overflow/go-orderedmap"
)

type processTime struct {
	time   time.Time
	taskId string
}

type Scheduler struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	config Config

	Container Container
	Actuator  Actuator
	Store     Store

	finishedTask chan *Task

	enableProcessedCheck bool
	processedTask        map[string]bool
	taskProcessedTime    []processTime

	bufflen, head, tail, count int
	wg                         *stlextension.LimitWaitGroup
}

func NewScheduler(container Container, actuator Actuator, store Store, config Config) (*Scheduler, error) {
	if err := config.check(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	scheduler := &Scheduler{
		ctx:       ctx,
		cancel:    cancel,
		config:    config,
		Container: container,
		Actuator:  actuator,
		Store:     store,
		wg:        stlextension.NewLimitWaitGroup(20),
		head:      0,
		tail:      0,
		count:     0,
	}

	if config.EnableFinishedTaskList {
		scheduler.finishedTask = make(chan *Task, 1e4)
	}

	go scheduler.start()

	return scheduler, nil
}

func (s *Scheduler) start() {
	go s.schedulerTask()

	if s.config.EnableStateCallback {
		go s.updateCallbackTask()
	}

	if s.config.EnableStatePoll {
		go s.updateTaskStatus()
	}

	// 既轮训又回调
	if s.config.EnableStatePoll && s.config.EnableStateCallback {
		s.enableProcessedCheck = true
		s.processedTask = make(map[string]bool)
		s.bufflen = 1e4
		s.taskProcessedTime = make([]processTime, s.bufflen)
		go s.cleanProcessTask()
	}
}

func (s *Scheduler) finished(task *Task) {
	task.EndedAt = time.Now()

	if !s.config.EnableFinishedTaskList {
		return
	}

	select {
	case s.finishedTask <- task:
		return
	default:
	}

	// Channel 已满，丢弃最旧的任务
	select {
	case <-s.finishedTask:
	default:
	}

	// 再次尝试发送最新任务
	select {
	case s.finishedTask <- task:
	default:
	}
}

func (s *Scheduler) success(ctx context.Context, task *Task) error {
	if err := s.Container.ToSuccessStatus(ctx, task); err != nil {
		if s.Store != nil {
			_ = s.Store.Delete(ctx, task)
		}

		return s.failed(ctx, task, err)
	}

	s.finished(task)

	return nil
}

func (s *Scheduler) failed(ctx context.Context, task *Task, err error) error {
	if err := s.Container.ToFailedStatus(ctx, task, err); err != nil {
		return err
	}

	s.finished(task)
	return nil
}

func (s *Scheduler) export(ctx context.Context, task *Task) {
	if s.Store == nil {
		_ = s.success(ctx, task)
	}

	if err := s.Container.ToExportStatus(ctx, task); err != nil {
	}
}

func (s *Scheduler) scheduleOnce(ctx context.Context) {
	runningCount, err := s.Container.GetRunningTaskCount(ctx)
	if err != nil {
		return
	}

	if runningCount >= s.config.TaskLimit {
		return
	}

	waitingTasks, err := s.Container.GetWaitingTask(ctx, s.config.TaskLimit-runningCount)
	if err != nil {
		return
	}

	wg := stlextension.NewLimitWaitGroup(20)
	for i := range waitingTasks {
		task := waitingTasks[i]
		wg.Add(1)

		scheduleFunc := func() {
			defer wg.Done()

			ignore, err := s.Actuator.Start(ctx, task)

			if err != nil && !ignore {
				_ = s.failed(s.ctx, task, err)
				return
			}

			// 进入运行状态失败
			if err := s.Container.ToRunningStatus(ctx, task); err != nil {
				_ = s.Actuator.Stop(ctx, task)
				_ = s.failed(s.ctx, task, err)
				return
			}
		}

		go scheduleFunc()
	}

	wg.Wait()
}

func (s *Scheduler) schedulerTask() {
	if s.config.SchedulerPollInterval == 0 {
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				s.scheduleOnce(s.ctx)
			}
		}
	}

	ticker := time.NewTicker(s.config.SchedulerPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.scheduleOnce(s.ctx)
		}
	}
}

func (s *Scheduler) cleanProcessTask() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	cleanFunc := func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		expireTime := time.Now().Add(-time.Second)

		for s.count > 0 {
			item := s.taskProcessedTime[s.head]

			if !item.time.Before(expireTime) {
				break
			}

			delete(s.processedTask, item.taskId)

			s.head = (s.head + 1) % s.bufflen
			s.count--
		}
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			cleanFunc()
		}
	}
}

func (s *Scheduler) checkProcessed(task *Task) bool {
	if !s.enableProcessedCheck {
		return true
	}

	if task.Status == TaskStatusRunning {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, existed := s.processedTask[task.Id]; !existed {
		return false
	}

	if s.count >= s.bufflen {
		oldest := s.taskProcessedTime[s.head]
		delete(s.processedTask, oldest.taskId)

		s.head = (s.head + 1) % s.bufflen
		s.count--
	}

	s.processedTask[task.Id] = true
	s.taskProcessedTime[s.tail] = processTime{
		time:   time.Now(),
		taskId: task.Id,
	}

	s.tail = (s.tail + 1) % s.bufflen
	s.count++
	return true
}

func (s *Scheduler) updateOnce(ctx context.Context) {
	tasks, err := s.Container.GetRunningTask(ctx)
	if err != nil || len(tasks) == 0 {
		return
	}

	statuses, err := s.Actuator.GetAsyncTaskStatus(ctx, tasks)
	if err != nil {
		return
	}

	if len(tasks) != len(statuses) {
		return
	}

	var wg sync.WaitGroup

	for i := range tasks {
		task, status := tasks[i], statuses[i]

		wg.Add(1)
		s.wg.Add(1)

		go func() {
			defer wg.Done()
			defer s.wg.Done()

			switch status.Status {
			case TaskStatusFailed:
				if !s.checkProcessed(task) {
					return
				}

				if task.TryTimes >= s.config.MaxTryTimes {
					_ = s.failed(ctx, task, errors.New(""))
					return
				}

				task.TryTimes++

				if _, err := s.Actuator.Start(ctx, task); err != nil {
					_ = s.failed(ctx, task, errors.New(""))
					return
				}

				if err := s.Container.ToRunningStatus(ctx, task); err != nil {
					_ = s.Actuator.Stop(ctx, task)
					_ = s.failed(ctx, task, errors.New(""))
				}

			case TaskStatusSuccess:
				if !s.checkProcessed(task) {
					return
				}

				s.export(ctx, task)

			case TaskStatusRunning:
				if s.config.Timeout > 0 && time.Since(task.StartedAt) > s.config.Timeout {
					if err := s.failed(ctx, task, errors.New("")); err != nil {
						_ = s.Actuator.Stop(ctx, task)
					}

					return
				}

				_ = s.Container.UpdateRunningTaskStatus(ctx, task, status)
			}
		}()
	}

	wg.Wait()
}

func (s *Scheduler) updateTaskStatus() {
	if s.config.StatePollInterval == 0 {
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				s.updateOnce(s.ctx)
			}
		}
	}

	ticker := time.NewTicker(s.config.StatePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.updateOnce(s.ctx)
		}
	}
}

func (s *Scheduler) updateCallbackTask() {
	for task := range s.config.CallbackReceiver.GetCallbackChannel(s.ctx) {
		if !s.checkProcessed(task) {
			continue
		}

		s.wg.Add(1)

		go func() {
			defer s.wg.Done()

			switch task.Status {
			case TaskStatusFailed:
				if task.TryTimes >= s.config.MaxTryTimes {
					_ = s.failed(s.ctx, task, errors.New(""))
					return
				}

				task.TryTimes++

				if _, err := s.Actuator.Start(s.ctx, task); err != nil {
					_ = s.failed(s.ctx, task, errors.New(""))
					return
				}

				if err := s.Container.ToRunningStatus(s.ctx, task); err != nil {
					_ = s.Actuator.Stop(s.ctx, task)
					_ = s.failed(s.ctx, task, errors.New(""))
				}
			case TaskStatusSuccess:
				s.export(s.ctx, task)
			}
		}()
	}
}
