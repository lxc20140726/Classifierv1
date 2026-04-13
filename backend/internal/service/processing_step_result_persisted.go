package service

import (
	"fmt"
	"strings"

	"github.com/liqiye/classifier/internal/repository"
)

func extractPersistedProcessingStepResults(nodeRun *repository.NodeRun) ([]ProcessingStepResult, error) {
	if nodeRun == nil {
		return nil, fmt.Errorf("node run is nil")
	}
	if strings.TrimSpace(nodeRun.OutputJSON) == "" {
		return nil, fmt.Errorf("node run %q output json is empty", nodeRun.ID)
	}

	typedOutputs, typed, err := parseTypedNodeOutputs(nodeRun.OutputJSON)
	if err != nil {
		return nil, fmt.Errorf("parse node output json for node run %q: %w", nodeRun.ID, err)
	}
	if !typed {
		return nil, fmt.Errorf("node run %q output is not typed output payload", nodeRun.ID)
	}

	value, ok := typedOutputs["step_results"]
	if !ok {
		return nil, fmt.Errorf("node run %q output missing step_results", nodeRun.ID)
	}
	results := processingStepResultsFromAny(value.Value)
	if len(results) == 0 {
		return nil, fmt.Errorf("node run %q step_results is empty", nodeRun.ID)
	}

	return results, nil
}
