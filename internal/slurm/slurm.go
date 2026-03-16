package slurm

import (
	"fmt"
	"slices"
	"strings"
	"os/exec"
	"time"
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
	
	// Poll and wait until the job is done
	pollCommand := fmt.Sprintf("sacct -j %s --format=State --noheader | awk 'NR==1{print $1}'", jobId)
	for {
		time.Sleep(time.Minute)
		output, err := exec.Command("sh", "-c", pollCommand).Output()
		if err != nil {
			return err
		}
		state := strings.TrimSpace(string(output))
		switch state {
		case "COMPLETED":
			return nil
		case "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED", "REVOKED":
			return fmt.Errorf("job with id %s failed: %s", jobId, state)
		case "PENDING", "RUNNING", "SUSPENDED", "REQUEUED", "":
			continue
		default:
			return fmt.Errorf("job with id %s reached an unhandled state: %s", jobId, state)
		}
	}
}