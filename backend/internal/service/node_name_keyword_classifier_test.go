package service

import (
	"context"
	"testing"

	"github.com/liqiye/classifier/internal/repository"
)

func TestNameKeywordClassifierBatch(t *testing.T) {
	t.Parallel()

	executor := newNameKeywordClassifierExecutor()
	output, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{Type: executor.Type()},
		Inputs: testInputs(map[string]any{
			"trees": []FolderTree{
				{Path: "/src/m1", Name: "進撃の巨人漫画"},
				{Path: "/src/p1", Name: "夏日写真集"},
				{Path: "/src/x1", Name: "random-folder"},
			},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if output.Status != ExecutionSuccess {
		t.Fatalf("status = %q, want success", output.Status)
	}
	if len(output.Outputs) != 2 {
		t.Fatalf("len(outputs) = %d, want 2", len(output.Outputs))
	}

	signals, ok := output.Outputs["signal"].Value.([]ClassificationSignal)
	if !ok {
		t.Fatalf("signals type = %T, want []ClassificationSignal", output.Outputs["signal"].Value)
	}
	if len(signals) != 3 {
		t.Fatalf("len(signals) = %d, want 3", len(signals))
	}
	if signals[0].SourcePath != "/src/m1" || signals[0].Category != "manga" || signals[0].Confidence != 1.0 {
		t.Fatalf("signal[0] = %+v, want manga hit with source path", signals[0])
	}
	if signals[1].SourcePath != "/src/p1" || signals[1].Category != "photo" || signals[1].Confidence != 0.9 {
		t.Fatalf("signal[1] = %+v, want photo hit with source path", signals[1])
	}
	if signals[2].SourcePath != "/src/x1" || !signals[2].IsEmpty || signals[2].Category != "" {
		t.Fatalf("signal[2] = %+v, want empty signal with source path", signals[2])
	}

	unresolved, ok := output.Outputs["pass"].Value.([]FolderTree)
	if !ok {
		t.Fatalf("unresolved type = %T, want []FolderTree", output.Outputs["pass"].Value)
	}
	if len(unresolved) != 1 || unresolved[0].Path != "/src/x1" {
		t.Fatalf("unresolved = %+v, want only /src/x1", unresolved)
	}
}

func TestNameKeywordClassifierSingleTreeInput(t *testing.T) {
	t.Parallel()

	executor := newNameKeywordClassifierExecutor()
	output, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{Type: executor.Type()},
		Inputs: testInputs(map[string]any{
			"trees": FolderTree{Path: "/src/comic", Name: "My Comic Collection"},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	signals, ok := output.Outputs["signal"].Value.([]ClassificationSignal)
	if !ok {
		t.Fatalf("signals type = %T, want []ClassificationSignal", output.Outputs["signal"].Value)
	}
	if len(signals) != 1 || signals[0].Category != "manga" || signals[0].SourcePath != "/src/comic" {
		t.Fatalf("signals = %+v, want one manga signal", signals)
	}
}
