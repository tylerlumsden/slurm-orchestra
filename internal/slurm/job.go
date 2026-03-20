package slurm

import (
	"sync"
)

type ChainItem interface {
	Run(manager *jobManager, ctx Context) error
}

type ChainType string
const (
	Sequential ChainType = "sequential"
	Parallel ChainType = "parallel"
)

type Chain struct {
	Type ChainType
	Items []ChainItem
	Args []string
}

type Job struct {
	Commands []string
	Args []string
}

type Context struct {
	SendChan SendChannel
}

func Run(item ChainItem) error {
	manager := GetJobManager(1000)
	send := manager.Register()
	defer manager.Unregister(send)

	return item.Run(manager, Context{SendChan: send})
}

func (j Job) Run(manager *jobManager, ctx Context) error {
	ctx.SendChan.jobChannel <- j
	err := <- ctx.SendChan.errChannel
	return err
}

func (c Chain) Run(manager *jobManager, ctx Context) error {
	switch c.Type {
	case Sequential:
		for _, item := range c.Items {
			if err := item.Run(manager, ctx); err != nil {
				return err
			}
		}
	case Parallel:
		var wg sync.WaitGroup
		wg.Add(len(c.Items) - 1)
		localErrorChannel := make(chan error, len(c.Items) - 1)
		var errs []error
		for i, item := range c.Items {
			if i != 0 {
				newCtx := ctx
				newCtx.SendChan = manager.Register()
				go func(item ChainItem, ctx Context) {
					defer wg.Done()
					err := item.Run(manager, ctx)
					if err != nil {
						localErrorChannel <- err
					}
				}(item, newCtx)
			} else {
				if err := item.Run(manager, ctx); err != nil {
					errs = append(errs, err)
				}
			}
		}
		wg.Wait()
		close(localErrorChannel)
		for err := range localErrorChannel {
			errs = append(errs, err)
		}
		if len(errs) != 0 {
			return errs[0]
		}
	}
	return nil
}