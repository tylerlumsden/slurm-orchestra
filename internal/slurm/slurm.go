package slurm

import (
	"fmt"
	"slices"
	"strings"
	"os/exec"
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

func sbatch(script string) (string, error) {
	command := fmt.Sprintf("#!/bin/sh\n%s\n", script)

	fmt.Println("Submitting sbatch script:\n" + command)

	cmd := exec.Command("sbatch")
	cmd.Stdin = strings.NewReader(command)
	jobId, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(jobId), nil
}

func (c Chain) Execute() error {
	fmt.Println("Chain!")
	return nil
}
func (j Job) Execute() error {
	args := j.Args 
	for i := range args {
		args[i] = fmt.Sprintf("#SBATCH %s", args[i])
	}

	// TODO resolve variables in the commands via context
	commands := j.Commands

	script := strings.Join(slices.Concat(args, commands), "\n")
	jobId, err := sbatch(script)
	if err != nil {
		return err
	}
	
	fmt.Println(jobId)
	// Poll and wait until the job is done
	return nil
}