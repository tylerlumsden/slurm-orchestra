package slurm

import (
	"fmt"
)

type ChainItem interface {
	Execute() error
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

func (c Chain) Execute() error {
	fmt.Println("Chain!")
	return nil
}
func (j Job) Execute() error {
	fmt.Println(j.Args)
	return nil
}