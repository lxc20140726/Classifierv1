package service

import (
	"context"
	"testing"
)

func TestCollectNodeExecutorCollectsDynamicInputsInNumericOrder(t *testing.T) {
	t.Parallel()

	out, err := newCollectNodeExecutor().Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{
			"items_10": []ProcessingItem{{FolderName: "ten"}},
			"items_2":  []ProcessingItem{{FolderName: "two"}},
			"items_6":  []ProcessingItem{{FolderName: "six"}},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	items, ok := out.Outputs["items"].Value.([]ProcessingItem)
	if !ok {
		t.Fatalf("items output type = %T, want []ProcessingItem", out.Outputs["items"].Value)
	}
	if len(items) != 3 {
		t.Fatalf("len(items) = %d, want 3", len(items))
	}

	got := []string{items[0].FolderName, items[1].FolderName, items[2].FolderName}
	want := []string{"two", "six", "ten"}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("items order = %v, want %v", got, want)
		}
	}
}
