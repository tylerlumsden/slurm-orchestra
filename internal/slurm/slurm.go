package slurm

import (
	"fmt"
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

	var stderr strings.Builder
	cmd := exec.Command("sbatch", "--parsable")
	cmd.Stdin = strings.NewReader(command)
	cmd.Stderr = &stderr
	jobIdRaw, err := cmd.Output()
	if err != nil {
		if stderr.Len() > 0 {
			fmt.Printf("%s\n", stderr.String())
		}
		return "", err
	}
	return strings.TrimSpace(string(jobIdRaw)), nil
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

	// TODO: resolve variables in the commands via context
	commands := j.Commands

	script := strings.Join(append(args, commands...), "\n")
	jobId, err := sbatch(script)
	if err != nil {
		return err
	}
	
	// TODO: Need to validate that the jobId is a valid job id.
	// 		 If the jobId is not valid, the program will currently enter an infinite loop
	// Poll and wait until the job is done
	pollCommand := fmt.Sprintf("sacct -j %s --format=State --noheader | awk 'NR==1{print $1}'", jobId)
	for {
		fmt.Printf("Polling job %s...\n", jobId)
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