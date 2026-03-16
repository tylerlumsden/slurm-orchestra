package parser

import (
	"fmt"
	"os"

	"github.com/tylerlumsden/slurm-orchestra/internal/slurm"
	"gopkg.in/yaml.v3"
)

type yamlNode map[string]any

func Parse(path string) (slurm.ChainItem, error) {
	fmt.Println("Parsing " + path)

	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var node yamlNode
	if err := yaml.Unmarshal(file, &node); err != nil {
		return nil, err
	}

	chain, err := parseYaml(node)
	if err != nil {
		return nil, err
	}
	return chain, nil
}

func toSlice(key string, val interface{}) ([]any, error) {
	raw, ok := val.([]any)
	if !ok {
		return nil, fmt.Errorf("expected a list for key %s", key)
	}
	return raw, nil
}

func toStringSlice(key string, val interface{}) ([]string, error) {
	raw, err := toSlice(key, val)
	if err != nil {
		return nil, err
	}

	result := make([]string, len(raw))
	for i, value := range raw {
		str, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string at index %d for key %s", i, key)
		}
		result[i] = str
	}
	return result, nil
}

func parseYaml(node yamlNode) (slurm.ChainItem, error) {
	if _, ok := node["cmds"]; ok {
		item := slurm.Job{}

		for key, value := range node {
			switch key {

			case "cmds":
				if list, err := toStringSlice(key, value); err == nil {
					item.Commands = list
				} else {
					return nil, err
				}

			default:
				item.Args = append(item.Args, fmt.Sprintf("%v", value))
			}
		}

		return &item, nil
	} 
	
	if _, ok := node["jobs"]; ok {
		item := slurm.Chain{}

		for key, value := range node {
			switch key {

			case "jobs":
				list, err := toSlice(key, value)
				if err != nil {
					return nil, err
				}
				for _, newNode := range list {
					new_item, err := parseYaml(newNode.(yamlNode))
					if err != nil {
						return nil, err
					}
					item.Items = append(item.Items, new_item)
				}

			default:
				item.Args = append(item.Args, fmt.Sprintf("%s=%v", key, value))
			}
		}
		
		return &item, nil
	} 

	return nil, fmt.Errorf("no key found to derive the value of node")
}