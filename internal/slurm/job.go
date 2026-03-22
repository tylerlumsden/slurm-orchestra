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
	WorkPool int
	WorkVar  string
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
	switch c.Type {
	case Sequential:
		for i := c.Range.Begin; i <= c.Range.End; i = i + c.Range.Step {
			if c.Range.RangeVar != "" {
				ctx.Vars[c.Range.RangeVar] = fmt.Sprintf("%d", i)
			}
			for _, item := range c.Items {
				if err := item.Run(manager, ctx); err != nil {
					return err
				}
			}
		}
	case Parallel:
		var wg sync.WaitGroup
		rangeCount := (c.Range.End-c.Range.Begin)/c.Range.Step + 1
		localErrorChannel := make(chan error, rangeCount*len(c.Items))
		var errs []error

		var idPool chan int
		if c.Range.WorkPool > 0 {
			idPool = make(chan int, c.Range.WorkPool)
			for id := 0; id < c.Range.WorkPool; id++ {
				idPool <- id
			}
		}

		for outerIndex := c.Range.Begin; outerIndex <= c.Range.End; outerIndex = outerIndex + c.Range.Step {
			if c.Range.RangeVar != "" {
				ctx.Vars[c.Range.RangeVar] = fmt.Sprintf("%d", outerIndex)
			}
			for innerIndex, item := range c.Items {
				newCtx := ctx
				newCtx.Vars = maps.Clone(ctx.Vars)
				if outerIndex != 0 || innerIndex != 0 {
					newCtx.SendChan = manager.Register()
				}
				wg.Add(1)
				go func(item ChainItem, ctx Context) {
					defer wg.Done()
					if idPool != nil {
						workID := <-idPool
						defer func() { idPool <- workID }()
						workVar := "work_id"
						if c.Range.WorkVar != "" {
							workVar = c.Range.WorkVar
						}
						ctx.Vars[workVar] = fmt.Sprintf("%d", workID)
					}
					err := item.Run(manager, ctx)
					if err != nil {
						localErrorChannel <- err
					}
				}(item, newCtx)
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