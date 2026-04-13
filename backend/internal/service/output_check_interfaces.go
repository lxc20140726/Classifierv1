package service

import (
	"context"

	"github.com/liqiye/classifier/internal/repository"
)

type SourceManifestBuilder interface {
	Build(ctx context.Context, folderID string) error
	EnsureForWorkflowRun(ctx context.Context, workflowRunID string, items []ProcessingItem) error
}

type OutputMappingBuilder interface {
	Build(ctx context.Context, workflowRunID string) error
}

type OutputValidator interface {
	ValidateWorkflowRun(ctx context.Context, workflowRunID string) ([]*repository.FolderOutputCheck, error)
	ValidateFolder(ctx context.Context, folderID string) (*repository.FolderOutputCheck, error)
	GetLatestDetail(ctx context.Context, folderID string) (*FolderOutputCheckDetail, error)
	CanMarkDone(ctx context.Context, folderID string) (bool, error)
}

type FolderCompletionUpdater interface {
	Sync(ctx context.Context, folderID string, check *repository.FolderOutputCheck) error
	MarkPending(ctx context.Context, folderID string) error
}

type FolderOutputCheckDetail struct {
	FolderID    string                              `json:"folder_id"`
	Summary     repository.FolderOutputCheckSummary `json:"summary"`
	Check       *repository.FolderOutputCheck       `json:"check,omitempty"`
	Mappings    []*repository.FolderOutputMapping   `json:"mappings"`
	Errors      []repository.OutputCheckError       `json:"errors"`
	SourceFiles []*repository.FolderSourceManifest  `json:"source_files"`
}
