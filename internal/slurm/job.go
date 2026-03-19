package slurm

import (
)

type ChainItem interface {
	Run() error
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