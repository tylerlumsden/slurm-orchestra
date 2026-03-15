package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	//"github.com/tylerlumsden/slurm-orchestra/internal"
)

var rootCmd = &cobra.Command {
	Use: "orch",
	Short: "Orchestra is a Slurm orchestrator which manages and runs a set of jobs from a YAML file",
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		yamlFile := args[0]
		fmt.Println(yamlFile)

		return nil
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println("An error occured while executing orch")
	}
}