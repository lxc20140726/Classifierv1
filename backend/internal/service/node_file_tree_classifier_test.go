package service

import (
	"context"
	"testing"
)

func TestFileTreeClassifierBatch(t *testing.T) {
	t.Parallel()

	executor := newFileTreeClassifierExecutor()
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{
			"trees": []FolderTree{
				{Path: "/src/manga", Files: []FileEntry{{Name: "archive.cbz", Ext: "cbz"}}},
				{Path: "/src/video", Files: []FileEntry{{Name: "movie.mkv", Ext: "mkv"}, {Name: "movie.srt", Ext: "srt"}}},
				{Path: "/src/unknown", Files: []FileEntry{{Name: "doc.pdf", Ext: "pdf"}}},
			},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Status != ExecutionSuccess {
		t.Fatalf("status = %q, want success", out.Status)
	}
	if len(out.Outputs) != 2 {
		t.Fatalf("len(outputs) = %d, want 2", len(out.Outputs))
	}

	signals, ok := out.Outputs["signal"].Value.([]ClassificationSignal)
	if !ok {
		t.Fatalf("port0 type = %T, want []ClassificationSignal", out.Outputs["signal"].Value)
	}
	if len(signals) != 3 {
		t.Fatalf("len(signals) = %d, want 3", len(signals))
	}
	if signals[0].Category != "manga" || signals[0].SourcePath != "/src/manga" {
		t.Fatalf("signal[0] = %+v, want manga with source path", signals[0])
	}
	if signals[1].Category != "video" || signals[1].SourcePath != "/src/video" {
		t.Fatalf("signal[1] = %+v, want video with source path", signals[1])
	}
	if !signals[2].IsEmpty || signals[2].SourcePath != "/src/unknown" {
		t.Fatalf("signal[2] = %+v, want empty with source path", signals[2])
	}

	unresolved, ok := out.Outputs["pass"].Value.([]FolderTree)
	if !ok {
		t.Fatalf("port1 type = %T, want []FolderTree", out.Outputs["folder"].Value)
	}
	if len(unresolved) != 1 || unresolved[0].Path != "/src/unknown" {
		t.Fatalf("unresolved = %+v, want only /src/unknown", unresolved)
	}
}

func TestFileTreeClassifierSingleTreeInput(t *testing.T) {
	t.Parallel()

	executor := newFileTreeClassifierExecutor()
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{"trees": FolderTree{Path: "/src/m", Files: []FileEntry{{Name: "archive.cbz", Ext: "cbz"}}}}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	signals, ok := out.Outputs["signal"].Value.([]ClassificationSignal)
	if !ok {
		t.Fatalf("port0 type = %T, want []ClassificationSignal", out.Outputs["signal"].Value)
	}
	if len(signals) != 1 || signals[0].Category != "manga" {
		t.Fatalf("signals = %+v, want one manga signal", signals)
	}
}
