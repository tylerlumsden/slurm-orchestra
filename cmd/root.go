package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/tylerlumsden/slurm-orchestra/internal/parser"
)

var rootCmd = &cobra.Command {
	Use: "orch",
	Short: "Orchestra is a Slurm orchestrator which manages and runs a set of jobs from a YAML file",
	Args: cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		file := args[0]
		chain, err := parser.Parse(file)
		if err != nil {
			return err
		}
		err = chain.Execute()
		if err != nil {
			return err
		}
		return nil
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println("\nAn error occured while executing orch")
	}
}