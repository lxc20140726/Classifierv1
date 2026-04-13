package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liqiye/classifier/internal/repository"
)

const (
	folderLineageNodeTypeOrigin         = "origin"
	folderLineageNodeTypeHistoricalPath = "historical_path"
	folderLineageNodeTypeCurrentPath    = "current_path"
	folderLineageNodeTypeArtifact       = "artifact"

	folderLineageEdgeTypeMovedTo   = "moved_to"
	folderLineageEdgeTypeRenamedTo = "renamed_to"
	folderLineageEdgeTypeProduced  = "produced"

	folderLineageEventTypeScanDiscovered = "scan_discovered"
	folderLineageEventTypeMove           = "move"
	folderLineageEventTypeRename         = "rename"
	folderLineageEventTypeRollback       = "rollback"
	folderLineageEventTypeArtifactCreate = "artifact_created"
	folderLineageEventTypeProcessingFail = "processing_failed"
)

type folderLineageFolderReader interface {
	GetByID(ctx context.Context, id string) (*repository.Folder, error)
	ListPathObservationsByFolderID(ctx context.Context, folderID string) ([]*repository.FolderPathObservation, error)
}

type folderLineageSnapshotReader interface {
	ListByFolderID(ctx context.Context, folderID string) ([]*repository.Snapshot, error)
}

type folderLineageReviewReader interface {
	GetLatestByFolderID(ctx context.Context, folderID string) (*repository.ProcessingReviewItem, error)
}

type folderLineageAuditReader interface {
	List(ctx context.Context, filter repository.AuditListFilter) ([]*repository.AuditLog, int, error)
}

type folderLineageOutputMappingReader interface {
	GetLatestWorkflowRunIDByFolderID(ctx context.Context, folderID string) (string, error)
	ListByWorkflowRunAndFolderID(ctx context.Context, workflowRunID, folderID string) ([]*repository.FolderOutputMapping, error)
}

type folderLineageSourceManifestReader interface {
	ListLatestByFolderID(ctx context.Context, folderID string) ([]*repository.FolderSourceManifest, error)
	ListByFolderID(ctx context.Context, folderID string) ([]*repository.FolderSourceManifest, error)
}

type FolderLineageService struct {
	folders   folderLineageFolderReader
	snapshots folderLineageSnapshotReader
	reviews   folderLineageReviewReader
	audits    folderLineageAuditReader
	manifests folderLineageSourceManifestReader
	mappings  folderLineageOutputMappingReader
}

func NewFolderLineageService(
	folderRepo folderLineageFolderReader,
	snapshotRepo folderLineageSnapshotReader,
	reviewRepo folderLineageReviewReader,
	auditRepo folderLineageAuditReader,
	sourceManifestRepo folderLineageSourceManifestReader,
	mappingRepo folderLineageOutputMappingReader,
) *FolderLineageService {
	return &FolderLineageService{
		folders:   folderRepo,
		snapshots: snapshotRepo,
		reviews:   reviewRepo,
		audits:    auditRepo,
		manifests: sourceManifestRepo,
		mappings:  mappingRepo,
	}
}

type FolderLineageResponse struct {
	Folder   *repository.Folder           `json:"folder"`
	Summary  FolderLineageSummary         `json:"summary"`
	Graph    FolderLineageGraph           `json:"graph"`
	Flow     *FolderLineageFlow           `json:"flow,omitempty"`
	Timeline []FolderLineageTimelineEvent `json:"timeline"`
	Review   *FolderLineageReview         `json:"review,omitempty"`
}

type FolderLineageSummary struct {
	OriginalPath    string     `json:"original_path"`
	CurrentPath     string     `json:"current_path"`
	Status          string     `json:"status"`
	Category        string     `json:"category"`
	LastProcessedAt *time.Time `json:"last_processed_at,omitempty"`
}

type FolderLineageGraph struct {
	Nodes []FolderLineageNode `json:"nodes"`
	Edges []FolderLineageEdge `json:"edges"`
}

type FolderLineageNode struct {
	ID            string     `json:"id"`
	Type          string     `json:"type"`
	Label         string     `json:"label"`
	Path          string     `json:"path"`
	FirstSeenAt   *time.Time `json:"first_seen_at,omitempty"`
	LastSeenAt    *time.Time `json:"last_seen_at,omitempty"`
	StepTypes     []string   `json:"step_types,omitempty"`
	WorkflowRunID string     `json:"workflow_run_id,omitempty"`
	JobID         string     `json:"job_id,omitempty"`
}

type FolderLineageEdge struct {
	ID            string     `json:"id"`
	Type          string     `json:"type"`
	Source        string     `json:"source"`
	Target        string     `json:"target"`
	OccurredAt    *time.Time `json:"occurred_at,omitempty"`
	StepType      string     `json:"step_type,omitempty"`
	WorkflowRunID string     `json:"workflow_run_id,omitempty"`
	JobID         string     `json:"job_id,omitempty"`
}

type FolderLineageTimelineEvent struct {
	ID            string    `json:"id"`
	Type          string    `json:"type"`
	OccurredAt    time.Time `json:"occurred_at"`
	Title         string    `json:"title"`
	Description   string    `json:"description,omitempty"`
	PathFrom      string    `json:"path_from,omitempty"`
	PathTo        string    `json:"path_to,omitempty"`
	WorkflowRunID string    `json:"workflow_run_id,omitempty"`
	JobID         string    `json:"job_id,omitempty"`
	StepType      string    `json:"step_type,omitempty"`
}

type FolderLineageReview struct {
	WorkflowRunID string                    `json:"workflow_run_id"`
	JobID         string                    `json:"job_id"`
	Status        string                    `json:"status"`
	Before        map[string]any            `json:"before,omitempty"`
	After         map[string]any            `json:"after,omitempty"`
	Diff          map[string]any            `json:"diff,omitempty"`
	ExecutedSteps []FolderLineageReviewStep `json:"executed_steps"`
	UpdatedAt     time.Time                 `json:"updated_at"`
	ReviewedAt    *time.Time                `json:"reviewed_at,omitempty"`
}

type FolderLineageReviewStep struct {
	NodeType   string `json:"node_type"`
	NodeLabel  string `json:"node_label,omitempty"`
	Status     string `json:"status,omitempty"`
	SourcePath string `json:"source_path,omitempty"`
	TargetPath string `json:"target_path,omitempty"`
	Error      string `json:"error,omitempty"`
}

type FolderLineageFlow struct {
	SourceDirectory   FolderLineageFlowDirectory    `json:"source_directory"`
	TargetDirectories []FolderLineageFlowDirectory  `json:"target_directories"`
	SourceFiles       []FolderLineageFlowSourceFile `json:"source_files"`
	TargetFiles       []FolderLineageFlowTargetFile `json:"target_files"`
	Links             []FolderLineageFlowLink       `json:"links"`
}

type FolderLineageFlowDirectory struct {
	ID           string `json:"id,omitempty"`
	Path         string `json:"path"`
	Label        string `json:"label"`
	ArtifactType string `json:"artifact_type,omitempty"`
}

type FolderLineageFlowSourceFile struct {
	ID           string `json:"id"`
	DirectoryID  string `json:"directory_id"`
	Name         string `json:"name"`
	Path         string `json:"path"`
	RelativePath string `json:"relative_path"`
	SizeBytes    int64  `json:"size_bytes"`
}

type FolderLineageFlowTargetFile struct {
	ID            string `json:"id"`
	DirectoryID   string `json:"directory_id"`
	Name          string `json:"name"`
	Path          string `json:"path"`
	ArtifactType  string `json:"artifact_type,omitempty"`
	NodeType      string `json:"node_type,omitempty"`
	WorkflowRunID string `json:"workflow_run_id,omitempty"`
	JobID         string `json:"job_id,omitempty"`
}

type FolderLineageFlowLink struct {
	ID            string `json:"id"`
	SourceFileID  string `json:"source_file_id"`
	TargetFileID  string `json:"target_file_id"`
	WorkflowRunID string `json:"workflow_run_id,omitempty"`
	JobID         string `json:"job_id,omitempty"`
	NodeType      string `json:"node_type,omitempty"`
}

type folderLineagePathAccumulator struct {
	path          string
	firstSeenAt   time.Time
	hasFirstSeen  bool
	lastSeenAt    time.Time
	hasLastSeen   bool
	stepTypes     map[string]struct{}
	workflowRunID string
	jobID         string
}

type folderLineagePathTransition struct {
	from          string
	to            string
	edgeType      string
	eventType     string
	stepType      string
	occurredAt    time.Time
	workflowRunID string
	jobID         string
}

type folderLineagePathState struct {
	OriginalPath string `json:"original_path"`
	CurrentPath  string `json:"current_path"`
}

type folderLineageArtifact struct {
	path          string
	stepType      string
	occurredAt    time.Time
	workflowRunID string
	jobID         string
}

func (s *FolderLineageService) GetFolderLineage(ctx context.Context, folderID string) (*FolderLineageResponse, error) {
	folder, err := s.folders.GetByID(ctx, folderID)
	if err != nil {
		return nil, fmt.Errorf("folderLineage.GetFolderLineage get folder %q: %w", folderID, err)
	}

	observations, err := s.folders.ListPathObservationsByFolderID(ctx, folderID)
	if err != nil {
		return nil, fmt.Errorf("folderLineage.GetFolderLineage list observations for folder %q: %w", folderID, err)
	}

	snapshots := []*repository.Snapshot{}
	if s.snapshots != nil {
		items, listErr := s.snapshots.ListByFolderID(ctx, folderID)
		if listErr != nil {
			return nil, fmt.Errorf("folderLineage.GetFolderLineage list snapshots for folder %q: %w", folderID, listErr)
		}
		snapshots = items
	}

	var latestReview *repository.ProcessingReviewItem
	if s.reviews != nil {
		item, reviewErr := s.reviews.GetLatestByFolderID(ctx, folderID)
		if reviewErr != nil && !errors.Is(reviewErr, repository.ErrNotFound) {
			return nil, fmt.Errorf("folderLineage.GetFolderLineage get latest review for folder %q: %w", folderID, reviewErr)
		}
		if reviewErr == nil {
			latestReview = item
		}
	}

	reviewPayload := buildFolderLineageReview(latestReview)
	currentPath := normalizeLineagePath(folder.Path)
	pathAccByPath := buildPathAccumulators(observations, currentPath)

	transitions := buildSnapshotTransitions(snapshots)
	transitions = append(transitions, buildReviewTransitions(reviewPayload, transitions)...)
	if len(transitions) == 0 {
		transitions = buildObservationFallbackTransitions(pathAccByPath)
	}

	associatePathMetadata(pathAccByPath, transitions)
	originalPath := resolveOriginalPath(pathAccByPath, observations, transitions, currentPath)

	artifacts, err := s.buildArtifacts(ctx, folderID, latestReview, reviewPayload)
	if err != nil {
		return nil, err
	}

	nodes, pathNodeIDByPath, artifactNodeIDByPath := buildFolderLineageNodes(pathAccByPath, originalPath, currentPath, artifacts)
	edges := buildFolderLineageEdges(transitions, artifacts, pathNodeIDByPath, artifactNodeIDByPath, currentPath)

	timeline, err := s.buildTimeline(ctx, folderID, observations, transitions, artifacts)
	if err != nil {
		return nil, err
	}

	lastProcessedAt := resolveLastProcessedAt(latestReview, transitions, artifacts)
	flow, err := s.buildFlow(ctx, folderID, latestReview)
	if err != nil {
		return nil, err
	}
	return &FolderLineageResponse{
		Folder: folder,
		Summary: FolderLineageSummary{
			OriginalPath:    originalPath,
			CurrentPath:     currentPath,
			Status:          strings.TrimSpace(folder.Status),
			Category:        strings.TrimSpace(folder.Category),
			LastProcessedAt: lastProcessedAt,
		},
		Graph: FolderLineageGraph{
			Nodes: nodes,
			Edges: edges,
		},
		Flow:     flow,
		Timeline: timeline,
		Review:   reviewPayload,
	}, nil
}
