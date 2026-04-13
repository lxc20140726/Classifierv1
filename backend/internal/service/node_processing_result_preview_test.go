package service

import (
	"context"
	"testing"
)

func TestProcessingResultPreviewExecutorMissingInput(t *testing.T) {
	t.Parallel()

	executor := newProcessingResultPreviewExecutor()
	out, err := executor.Execute(context.Background(), NodeExecutionInput{Inputs: map[string]*TypedValue{}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Status != ExecutionFailure {
		t.Fatalf("status = %q, want failure", out.Status)
	}
	if out.ErrorCode != "NODE_INPUT_MISSING" {
		t.Fatalf("error code = %q, want NODE_INPUT_MISSING", out.ErrorCode)
	}
}

func TestProcessingResultPreviewExecutorEmptyInput(t *testing.T) {
	t.Parallel()

	executor := newProcessingResultPreviewExecutor()
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{"step_results": []ProcessingStepResult{}}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Status != ExecutionFailure {
		t.Fatalf("status = %q, want failure", out.Status)
	}
	if out.ErrorCode != "NODE_INPUT_EMPTY" {
		t.Fatalf("error code = %q, want NODE_INPUT_EMPTY", out.ErrorCode)
	}
}

func TestProcessingResultPreviewExecutorTypeError(t *testing.T) {
	t.Parallel()

	executor := newProcessingResultPreviewExecutor()
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{"step_results": "invalid"}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Status != ExecutionFailure {
		t.Fatalf("status = %q, want failure", out.Status)
	}
	if out.ErrorCode != "NODE_INPUT_TYPE" {
		t.Fatalf("error code = %q, want NODE_INPUT_TYPE", out.ErrorCode)
	}
}

func TestProcessingResultPreviewExecutorSuccess(t *testing.T) {
	t.Parallel()

	executor := newProcessingResultPreviewExecutor()
	results := []ProcessingStepResult{
		{SourcePath: "/src/a", TargetPath: "/dst/a", NodeType: "move-node", NodeLabel: "移动", Status: "moved"},
		{SourcePath: "/src/b", TargetPath: "/dst/b", NodeType: "move-node", NodeLabel: "移动", Status: "failed"},
		{SourcePath: "/src/c", TargetPath: "/dst/c", NodeType: "move-node", NodeLabel: "移动", Status: "succeeded"},
	}
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{"step_results": results}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Status != ExecutionSuccess {
		t.Fatalf("status = %q, want success", out.Status)
	}
	if _, exists := out.Outputs["step_results"]; exists {
		t.Fatalf("step_results output should not exist for preview node")
	}
	if _, exists := out.Outputs["summary"]; !exists {
		t.Fatalf("summary output should exist")
	}
}

func TestProcessingResultPreviewExecutorSummarySemantics(t *testing.T) {
	t.Parallel()

	executor := newProcessingResultPreviewExecutor()
	results := []ProcessingStepResult{
		{SourcePath: "/src/dir1", NodeType: "compress-node", NodeLabel: "压缩", Status: "succeeded"},
		{SourcePath: "/src/dir1", NodeType: "move-node", NodeLabel: "移动", Status: "moved"},
		{SourcePath: "/src/dir2", NodeType: "compress-node", NodeLabel: "压缩", Status: "failed", Error: "zip failed"},
		{SourcePath: "/src/dir2", NodeType: "move-node", NodeLabel: "移动", Status: "skipped"},
		{SourcePath: "/src/dir3", NodeType: "thumbnail-node", NodeLabel: "缩略图", Status: "error"},
	}
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{"step_results": results}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	summary, ok := out.Outputs["summary"].Value.(processingResultPreviewSummary)
	if !ok {
		t.Fatalf("summary output type = %T, want processingResultPreviewSummary", out.Outputs["summary"].Value)
	}
	if summary.TotalDirs != 3 {
		t.Fatalf("summary.TotalDirs = %d, want 3", summary.TotalDirs)
	}
	if summary.TotalSteps != 5 {
		t.Fatalf("summary.TotalSteps = %d, want 5", summary.TotalSteps)
	}
	if summary.Succeeded != 1 || summary.Failed != 2 {
		t.Fatalf("summary succeeded/failed = %d/%d, want 1/2", summary.Succeeded, summary.Failed)
	}
	if len(summary.ByDirectory) != 3 {
		t.Fatalf("summary.ByDirectory len = %d, want 3", len(summary.ByDirectory))
	}
	if summary.ByDirectory[0].SourcePath != "/src/dir2" || summary.ByDirectory[0].Failed != 2 {
		t.Fatalf("summary.ByDirectory[0] = %#v, want /src/dir2 with failed=2", summary.ByDirectory[0])
	}
	if len(summary.ByDirectory[0].Steps) != 2 {
		t.Fatalf("summary.ByDirectory[0].Steps len = %d, want 2", len(summary.ByDirectory[0].Steps))
	}
	if summary.ByDirectory[2].SourcePath != "/src/dir1" || summary.ByDirectory[2].Failed != 0 {
		t.Fatalf("summary.ByDirectory[2] = %#v, want /src/dir1 with failed=0", summary.ByDirectory[2])
	}
}
