package service

import (
	"context"
	"testing"

	"github.com/liqiye/classifier/internal/repository"
)

func TestConfidenceCheckExecutorBatch(t *testing.T) {
	t.Parallel()

	executor := newConfidenceCheckExecutor()
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{Config: map[string]any{"threshold": 0.75}},
		Inputs: testInputs(map[string]any{
			"signals": []ClassificationSignal{
				{SourcePath: "/src/high", Category: "video", Confidence: 0.9, Reason: "high"},
				{SourcePath: "/src/low", Category: "photo", Confidence: 0.5, Reason: "low"},
				{SourcePath: "/src/empty", IsEmpty: true},
			},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Status != ExecutionSuccess {
		t.Fatalf("status = %q, want success", out.Status)
	}

	high, ok := out.Outputs["high"].Value.([]ClassificationSignal)
	if !ok {
		t.Fatalf("high type = %T, want []ClassificationSignal", out.Outputs["high"].Value)
	}
	low, ok := out.Outputs["low"].Value.([]ClassificationSignal)
	if !ok {
		t.Fatalf("low type = %T, want []ClassificationSignal", out.Outputs["low"].Value)
	}
	if len(high) != 3 || len(low) != 3 {
		t.Fatalf("len(high/low) = %d/%d, want 3/3", len(high), len(low))
	}
	if high[0].Category != "video" || high[0].SourcePath != "/src/high" || low[0].IsEmpty != true {
		t.Fatalf("high[0]/low[0] = %+v / %+v, want high retained and low empty", high[0], low[0])
	}
	if low[1].Category != "photo" || low[1].SourcePath != "/src/low" || high[1].IsEmpty != true {
		t.Fatalf("high[1]/low[1] = %+v / %+v, want low retained and high empty", high[1], low[1])
	}
	if !high[2].IsEmpty || !low[2].IsEmpty {
		t.Fatalf("high[2]/low[2] = %+v / %+v, want both empty", high[2], low[2])
	}
}
