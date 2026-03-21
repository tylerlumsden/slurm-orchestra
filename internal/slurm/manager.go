package slurm

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	pollRetryCount  = 5
	pollRetryTime   = time.Second * 30
	managerLoopTime = time.Second * 30
)

type gate struct {
	mutex sync.Mutex
	cond  *sync.Cond
}

type workChannel struct {
	jobChannel chan Job
	errChannel chan error
}

type SendChannel struct {
	jobChannel chan<- Job
	errChannel <-chan error
}

type jobManager struct {
	channelCap   int
	channels     []workChannel
	registerGate gate
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

func Execute(j Job) (string, error) {
	var args []string
	for i := range j.Args {
		args = append(args, fmt.Sprintf("#SBATCH %s", j.Args[i]))
	}

	// TODO: resolve variables in the commands via context
	commands := j.Commands

	script := strings.Join(append(args, commands...), "\n")
	jobId, err := sbatch(script)
	if err != nil {
		return "", err
	}
	return jobId, nil
}

// Should probably implement throwing an error if more than one job manager gets created
// Or we could do a singleton pattern where we return the same manager as first created
// This is bad because we would not be respecting job submission limits in our logic, so we will get a lot of errors
func GetJobManager(capacity int) *jobManager {
	manager := &jobManager{channelCap: capacity}
	manager.registerGate.cond = sync.NewCond(&manager.registerGate.mutex)
	go manager.start()
	return manager
}

func pollState(jobIds []string) (map[string]string, error) {

	formattedIds := strings.Join(jobIds, ",")
	output, err := exec.Command("sacct", "-j", formattedIds, "-o", "JobID,State", "-n", "-X").Output()
	if err != nil {
		return nil, err
	}

	statuses := make(map[string]string)
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			statuses[fields[0]] = fields[1]
		}
	}

	// Verify that all job ids in the original list exist in the map
	for _, id := range jobIds {
		if _, ok := statuses[id]; !ok {
			return nil, fmt.Errorf("sacct returned no status for job id %s", id)
		}
	}
	return statuses, nil
}

func (manager *jobManager) start() {
	runningJobs := make(map[string]workChannel)
	for {
		manager.registerGate.mutex.Lock()
		for _, channel := range manager.channels {
			jobCount := len(channel.jobChannel)
			for i := 0; i < jobCount; i++ {
				job := <-channel.jobChannel
				jobId, err := Execute(job)
				if err != nil {
					channel.errChannel <- err
					continue
				}
				runningJobs[jobId] = channel
			}
		}
		manager.registerGate.mutex.Unlock()

		var statuses map[string]string
		var err error
		for i := 0; i < pollRetryCount; i++ {
			fmt.Printf("Polling %d job(s)...\n", len(runningJobs))
			keys := make([]string, 0, len(runningJobs))
			for key := range runningJobs {
				keys = append(keys, key)
			}
			statuses, err = pollState(keys)
			if err == nil {
				break
			}
			time.Sleep(pollRetryTime)
		}
		// If we cannot successfully get the status of all jobs after pollRetryCount times,
		// then something is wrong and we should not let the program continue
		// It's possible we may want to change this to send an error through some channel in the future
		if err != nil {
			log.Fatalf("Polling failed after %d retries: %v", pollRetryCount, err)
		}

		for id, channel := range runningJobs {
			state := statuses[id]
			switch state {
			case "COMPLETED":
				delete(runningJobs, id)
				channel.errChannel <- nil
			case "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED", "REVOKED":
				channel.errChannel <- fmt.Errorf("job with id %s failed: %s", id, state)
			case "PENDING", "RUNNING", "SUSPENDED", "REQUEUED":
				fmt.Printf("Job %s is %s...\n", id, state)
				continue
			default:
				channel.errChannel <- fmt.Errorf("job with id %s reached an unhandled state: %s", id, state)
			}
		}
		time.Sleep(managerLoopTime)
	}
}

func (manager *jobManager) Register() SendChannel {
	manager.registerGate.mutex.Lock()
	defer manager.registerGate.mutex.Unlock()

	for len(manager.channels) >= manager.channelCap {
		manager.registerGate.cond.Wait()
	}

	jobChannel := make(chan Job, 1)
	errChannel := make(chan error, 1)
	manager.channels = append(manager.channels, workChannel{jobChannel, errChannel})

	return SendChannel{jobChannel, errChannel}
}

func (manager *jobManager) Unregister(send SendChannel) {
	manager.registerGate.mutex.Lock()
	defer manager.registerGate.mutex.Unlock()

	for i, channel := range manager.channels {
		if (send.jobChannel) == (channel.jobChannel) {
			manager.channels = append(manager.channels[:i], manager.channels[i+1:]...)
			close(channel.jobChannel)
			close(channel.errChannel)
			manager.registerGate.cond.Signal()
			break
		}
	}
}
