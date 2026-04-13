package service

import (
	"context"
	"testing"
)

func TestSignalAggregatorPrefersDirectVideoOverPromoSubdirs(t *testing.T) {
	t.Parallel()

	executor := newSignalAggregatorExecutor()

	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{
			"trees": []FolderTree{{
				Path: "/src/5kporn.e20.melody.marks.5k",
				Name: "5kporn.e20.melody.marks.5k",
				Files: []FileEntry{
					{Name: "5kporn.e20.melody.marks.5k.mp4", Ext: ".mp4"},
				},
				Subdirs: []FolderTree{
					{
						Path: "/src/5kporn.e20.melody.marks.5k/2048",
						Name: "2048",
						Subdirs: []FolderTree{
							{
								Path: "/src/5kporn.e20.melody.marks.5k/2048/__photo",
								Name: "__photo",
								Files: []FileEntry{
									{Name: "promo.jpg", Ext: ".jpg"},
									{Name: "promo.gif", Ext: ".gif"},
								},
							},
							{
								Path: "/src/5kporn.e20.melody.marks.5k/2048/__unsupported",
								Name: "__unsupported",
								Files: []FileEntry{
									{Name: "spam.url", Ext: ".url"},
									{Name: "spam.txt", Ext: ".txt"},
								},
							},
						},
					},
					{
						Path: "/src/5kporn.e20.melody.marks.5k/宣传文件",
						Name: "宣传文件",
						Files: []FileEntry{
							{Name: "promo-2.jpg", Ext: ".jpg"},
							{Name: "promo-3.gif", Ext: ".gif"},
							{Name: "readme.url", Ext: ".url"},
						},
					},
				},
			}},
			"signal_ext": []ClassificationSignal{
				{
					SourcePath: "/src/5kporn.e20.melody.marks.5k",
					Category:   "video",
					Confidence: 0.85,
					Reason:     "ext-ratio:video",
				},
			},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	entries, ok := out.Outputs["entries"].Value.([]ClassifiedEntry)
	if !ok {
		t.Fatalf("entries output type = %T, want []ClassifiedEntry", out.Outputs["entries"].Value)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	if entries[0].Category != "video" {
		t.Fatalf("entry category = %q, want video", entries[0].Category)
	}
	if entries[0].Reason != "direct:video-only" {
		t.Fatalf("entry reason = %q, want direct:video-only", entries[0].Reason)
	}
}
