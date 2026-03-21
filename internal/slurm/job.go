package slurm

import (
	"sync"
	"fmt"
	"slices"
	"maps"
	"os"
	"strings"
)

type ChainItem interface {
	Run(manager *jobManager, ctx Context) error
}

type CustomRange struct {
	Begin int
	End int
	Step int
    RangeVar string
}

type ChainType string
const (
	Sequential ChainType = "sequential"
	Parallel ChainType = "parallel"
)

type Chain struct {
	Type ChainType
	Items []ChainItem
	Args map[string]string
	Range CustomRange
}

type Job struct {
	Commands []string
	Args map[string]string
}

type Context struct {
	SendChan SendChannel
	Vars map[string]string
	Args map[string]string
}

func CreateContext() Context {
	item := Context{}

	item.Vars = make(map[string]string)
	item.Args = make(map[string]string)

	return item
}

func CreateJob() Job {
	item := Job{}

	item.Args = make(map[string]string)

	return item
}

func CreateChain() Chain {
	item := Chain{}

	item.Type = Sequential
	item.Range.Begin = 1
	item.Range.End = 1
	item.Range.Step = 1
	item.Range.RangeVar = ""

	item.Args = make(map[string]string)

	return item
}

func Run(item ChainItem) error {
	manager := GetJobManager(1000)
	send := manager.Register()
	defer manager.Unregister(send)

	ctx := CreateContext()
	ctx.SendChan = send
	return item.Run(manager, ctx)
}

func resolve(str string, resolver map[string]string) (string, error) {
	var err error
	return os.Expand(str, func(key string) string {
		if val, ok := resolver[key]; ok {
			return val
		}

		if val := os.Getenv(key); val != "" {
			return val
		}

		if strings.HasPrefix(key, "SLURM_") {
			return "$" + key
		}

		err = fmt.Errorf("Unknown variable: $%s\n", key)
		return ""
	}), err
}

func (j Job) newResolved(ctx Context) (Job, error) {
	newJob := Job{
		Commands: slices.Clone(j.Commands),
		Args: maps.Clone(j.Args),
	}

	for key, value := range ctx.Args {
		newJob.Args[key] = value
	}

	for i := range newJob.Commands {
		resolvedCmd, err := resolve(newJob.Commands[i], ctx.Vars)
		if err != nil {
			return Job{}, err
		}
		newJob.Commands[i] = resolvedCmd
	}

	for key, value := range newJob.Args {
		resolvedArg, err := resolve(value, ctx.Vars)
		if err != nil {
			return Job{}, err
		}
		newJob.Args[key] = resolvedArg
	}

	return newJob, nil
}

func (j Job) Run(manager *jobManager, ctx Context) error {
	newJob, err := j.newResolved(ctx)
	if err != nil {
		return err
	}
	ctx.SendChan.jobChannel <- newJob
	err = <- ctx.SendChan.errChannel
	return err
}

func (c Chain) Run(manager *jobManager, ctx Context) error {
	for i := c.Range.Begin; i <= c.Range.End; i = i + c.Range.Step {
		if c.Range.RangeVar != "" {
			ctx.Vars[c.Range.RangeVar] = fmt.Sprintf("%d", i)
		}
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
	}
	return nil
}