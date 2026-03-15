package internal

type ChainItem interface {
	execute() error
}

type ChainType string
const (
	Sequential ChainType = "sequential"
	Parallel ChainType = "parallel"
)

type Chain struct {
	Type ChainType
	Items []ChainItem
}

type Job struct {
	Commands []string
	Args []string
}

func (c Chain) execute() error {
	return nil
}
func (j Job) execute() error {
	return nil
}