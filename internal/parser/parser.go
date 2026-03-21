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

func parseRange(node yamlNode) (slurm.CustomRange, error) {
	var newRange slurm.CustomRange

	if begin, ok := node["begin"].(int); ok {
		newRange.Begin = begin
	} else {
		return slurm.CustomRange{}, fmt.Errorf("Could not find begin for some range\n")
	}

	if end, ok := node["end"].(int); ok {
		newRange.End = end
	} else {
		return slurm.CustomRange{}, fmt.Errorf("Could not find end for some range\n")
	}

	if step, ok := node["step"].(int); ok {
		newRange.Step = step
	} else {
		newRange.Step = 1
	}

	if varName, ok := node["var"].(string); ok {
		newRange.RangeVar = varName
	} else {
		newRange.RangeVar = ""
	}

	return newRange, nil
}

type ChainHandler func(yamlNode, *slurm.Chain) error
func getRange(node yamlNode, item *slurm.Chain) error {
	if rangeNode, ok := node["range"]; ok {
		if innerNode, ok := rangeNode.(yamlNode); ok {
			if begin, ok := innerNode["begin"].(int); ok {
				item.Range.Begin = begin
			} else {
				return fmt.Errorf("range does not have a begin value\n")
			}

			if end, ok := innerNode["end"].(int); ok {
				item.Range.End = end
			} else {
				return fmt.Errorf("range does not have an end value\n")
			}

			// Optional
			if step, ok := innerNode["step"].(int); ok {
				item.Range.Step = step
			}

			// Optional
			if varName, ok := innerNode["var"].(string); ok {
				item.Range.RangeVar = varName
			}
		} else {
			return fmt.Errorf("range is formatted improperly\n")
		}
	}

	return nil
}

func getJobs(node yamlNode, item *slurm.Chain) error {
	list, err := toSlice("jobs", node["jobs"])
	if err != nil {
		return err
	}
	for _, newNode := range list {
		new_item, err := parseYaml(newNode.(yamlNode))
		if err != nil {
			return err
		}
		item.Items = append(item.Items, new_item)
	}

	return nil
}

func parseYaml(node yamlNode) (slurm.ChainItem, error) {
	// TODO: Refactor similarly to chain logic
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
				item.Args = append(item.Args, fmt.Sprintf("--%s=%v", key, value))
			}
		}

		return &item, nil
	} 

	chainHandlers := map[string]ChainHandler {
		"range": getRange,
		"jobs": getJobs,
	}
	if _, ok := node["jobs"]; ok {
		item := slurm.CreateChain()

		for key, value := range node {
			if handler, ok := chainHandlers[key]; ok {
				if err := handler(node, &item); err != nil {
					return nil, err
				} 
			} else {
				fmt.Println("Appending key" + key)
				item.Args = append(item.Args, fmt.Sprintf("--%s=%v", key, value))
			}
		}
		
		return &item, nil
	} 

	return nil, fmt.Errorf("no key found to derive the value of node")
}