package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/liqiye/classifier/internal/repository"
)

type stubLineageFolderRepo struct {
	folder       *repository.Folder
	observations []*repository.FolderPathObservation
}

func (s *stubLineageFolderRepo) GetByID(_ context.Context, _ string) (*repository.Folder, error) {
	if s.folder == nil {
		return nil, repository.ErrNotFound
	}
	return s.folder, nil
}

func (s *stubLineageFolderRepo) ListPathObservationsByFolderID(_ context.Context, _ string) ([]*repository.FolderPathObservation, error) {
	return s.observations, nil
}

type stubLineageSnapshotRepo struct {
	items []*repository.Snapshot
}

func (s *stubLineageSnapshotRepo) ListByFolderID(_ context.Context, _ string) ([]*repository.Snapshot, error) {
	return s.items, nil
}

type stubLineageReviewRepo struct {
	item *repository.ProcessingReviewItem
	err  error
}

func (s *stubLineageReviewRepo) GetLatestByFolderID(_ context.Context, _ string) (*repository.ProcessingReviewItem, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.item == nil {
		return nil, repository.ErrNotFound
	}
	return s.item, nil
}

type stubLineageAuditRepo struct {
	logs []*repository.AuditLog
}

func (s *stubLineageAuditRepo) List(_ context.Context, _ repository.AuditListFilter) ([]*repository.AuditLog, int, error) {
	return s.logs, len(s.logs), nil
}

type stubLineageOutputMappingRepo struct {
	latestWorkflowRunID string
	byWorkflowRun       map[string][]*repository.FolderOutputMapping
}

func (s *stubLineageOutputMappingRepo) GetLatestWorkflowRunIDByFolderID(_ context.Context, _ string) (string, error) {
	if s.latestWorkflowRunID == "" {
		return "", repository.ErrNotFound
	}
	return s.latestWorkflowRunID, nil
}

func (s *stubLineageOutputMappingRepo) ListByWorkflowRunAndFolderID(_ context.Context, workflowRunID, _ string) ([]*repository.FolderOutputMapping, error) {
	items := s.byWorkflowRun[workflowRunID]
	if len(items) == 0 {
		return nil, repository.ErrNotFound
	}
	return items, nil
}

type stubLineageSourceManifestRepo struct {
	items []*repository.FolderSourceManifest
}

func (s *stubLineageSourceManifestRepo) ListLatestByFolderID(_ context.Context, _ string) ([]*repository.FolderSourceManifest, error) {
	return s.items, nil
}

func (s *stubLineageSourceManifestRepo) ListByFolderID(_ context.Context, _ string) ([]*repository.FolderSourceManifest, error) {
	return s.items, nil
}

func testLineageTime(hour int) time.Time {
	return time.Date(2026, 1, 2, hour, 0, 0, 0, time.UTC)
}

func mustJSONRawMessage(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return raw
}

func newTestLineageService(
	folder *repository.Folder,
	observations []*repository.FolderPathObservation,
	snapshots []*repository.Snapshot,
	review *repository.ProcessingReviewItem,
	auditLogs []*repository.AuditLog,
	manifests []*repository.FolderSourceManifest,
	mappings *stubLineageOutputMappingRepo,
) *FolderLineageService {
	return NewFolderLineageService(
		&stubLineageFolderRepo{folder: folder, observations: observations},
		&stubLineageSnapshotRepo{items: snapshots},
		&stubLineageReviewRepo{item: review},
		&stubLineageAuditRepo{logs: auditLogs},
		&stubLineageSourceManifestRepo{items: manifests},
		mappings,
	)
}

func TestFolderLineageServiceOnlyCurrentPath(t *testing.T) {
	t.Parallel()

	svc := newTestLineageService(
		&repository.Folder{ID: "f1", Path: "/media/current", Status: "done", Category: "photo"},
		[]*repository.FolderPathObservation{{
			ID:          "o1",
			FolderID:    "f1",
			Path:        "/media/current",
			IsCurrent:   true,
			FirstSeenAt: testLineageTime(1),
			LastSeenAt:  testLineageTime(1),
		}},
		nil,
		nil,
		nil,
		nil,
		&stubLineageOutputMappingRepo{},
	)

	resp, err := svc.GetFolderLineage(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFolderLineage() error = %v", err)
	}
	if resp.Summary.OriginalPath != "/media/current" || resp.Summary.CurrentPath != "/media/current" {
		t.Fatalf("summary original/current = %q/%q, want /media/current", resp.Summary.OriginalPath, resp.Summary.CurrentPath)
	}
	if len(resp.Graph.Nodes) != 1 || resp.Graph.Nodes[0].Type != folderLineageNodeTypeCurrentPath {
		t.Fatalf("graph nodes = %#v, want one current_path node", resp.Graph.Nodes)
	}
}

func TestFolderLineageServiceMoveEdgeFromSnapshot(t *testing.T) {
	t.Parallel()

	svc := newTestLineageService(
		&repository.Folder{ID: "f1", Path: "/target/a", Status: "done", Category: "video"},
		[]*repository.FolderPathObservation{
			{ID: "o1", FolderID: "f1", Path: "/source/a", FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(1)},
			{ID: "o2", FolderID: "f1", Path: "/target/a", IsCurrent: true, FirstSeenAt: testLineageTime(2), LastSeenAt: testLineageTime(2)},
		},
		[]*repository.Snapshot{{
			ID:        "s1",
			FolderID:  "f1",
			JobID:     "job-1",
			Status:    "committed",
			CreatedAt: testLineageTime(2),
			After: mustJSONRawMessage(t, []map[string]string{{
				"original_path": "/source/a",
				"current_path":  "/target/a",
			}}),
		}},
		nil,
		nil,
		nil,
		&stubLineageOutputMappingRepo{},
	)

	resp, err := svc.GetFolderLineage(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFolderLineage() error = %v", err)
	}
	if len(resp.Graph.Edges) != 1 || resp.Graph.Edges[0].Type != folderLineageEdgeTypeMovedTo {
		t.Fatalf("graph edges = %#v, want one moved_to edge", resp.Graph.Edges)
	}
}

func TestFolderLineageServiceMultiMoveProducesContinuousChain(t *testing.T) {
	t.Parallel()

	svc := newTestLineageService(
		&repository.Folder{ID: "f1", Path: "/p3", Status: "done", Category: "mixed"},
		[]*repository.FolderPathObservation{
			{ID: "o1", FolderID: "f1", Path: "/p1", FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(1)},
			{ID: "o2", FolderID: "f1", Path: "/p2", FirstSeenAt: testLineageTime(2), LastSeenAt: testLineageTime(2)},
			{ID: "o3", FolderID: "f1", Path: "/p3", IsCurrent: true, FirstSeenAt: testLineageTime(3), LastSeenAt: testLineageTime(3)},
		},
		[]*repository.Snapshot{
			{ID: "s1", FolderID: "f1", Status: "committed", CreatedAt: testLineageTime(2), After: mustJSONRawMessage(t, []map[string]string{{"original_path": "/p1", "current_path": "/p2"}})},
			{ID: "s2", FolderID: "f1", Status: "committed", CreatedAt: testLineageTime(3), After: mustJSONRawMessage(t, []map[string]string{{"original_path": "/p2", "current_path": "/p3"}})},
		},
		nil,
		nil,
		nil,
		&stubLineageOutputMappingRepo{},
	)

	resp, err := svc.GetFolderLineage(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFolderLineage() error = %v", err)
	}
	if len(resp.Graph.Edges) != 2 {
		t.Fatalf("len(graph.edges) = %d, want 2", len(resp.Graph.Edges))
	}
}

func TestFolderLineageServiceRollbackEvent(t *testing.T) {
	t.Parallel()

	svc := newTestLineageService(
		&repository.Folder{ID: "f1", Path: "/old", Status: "pending", Category: "other"},
		[]*repository.FolderPathObservation{
			{ID: "o1", FolderID: "f1", Path: "/old", IsCurrent: true, FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(4)},
			{ID: "o2", FolderID: "f1", Path: "/new", FirstSeenAt: testLineageTime(2), LastSeenAt: testLineageTime(3)},
		},
		[]*repository.Snapshot{{
			ID:        "s1",
			FolderID:  "f1",
			Status:    "reverted",
			CreatedAt: testLineageTime(3),
			After: mustJSONRawMessage(t, []map[string]string{{
				"original_path": "/old",
				"current_path":  "/new",
			}}),
		}},
		nil,
		nil,
		nil,
		&stubLineageOutputMappingRepo{},
	)

	resp, err := svc.GetFolderLineage(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFolderLineage() error = %v", err)
	}
	foundRollback := false
	for _, event := range resp.Timeline {
		if event.Type == folderLineageEventTypeRollback {
			foundRollback = true
			break
		}
	}
	if !foundRollback {
		t.Fatalf("timeline missing rollback event: %#v", resp.Timeline)
	}
}

func TestFolderLineageServiceReviewArtifactsAndOutputMappingFallback(t *testing.T) {
	t.Parallel()

	review := &repository.ProcessingReviewItem{
		ID:            "r1",
		WorkflowRunID: "wr-1",
		JobID:         "job-1",
		FolderID:      "f1",
		Status:        "approved",
		AfterJSON: mustJSONRawMessage(t, map[string]any{
			"path":           "/current",
			"artifact_paths": []string{"/out/a.cbz"},
		}),
		StepResultsJSON: mustJSONRawMessage(t, []map[string]any{{
			"node_type":   "compress-node",
			"source_path": "/current",
			"target_path": "/out/a.cbz",
			"status":      "succeeded",
		}}),
		UpdatedAt: testLineageTime(5),
	}

	svcWithReview := newTestLineageService(
		&repository.Folder{ID: "f1", Path: "/current", Status: "done", Category: "manga"},
		[]*repository.FolderPathObservation{{ID: "o1", FolderID: "f1", Path: "/current", IsCurrent: true, FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(1)}},
		nil,
		review,
		nil,
		nil,
		&stubLineageOutputMappingRepo{},
	)
	resp, err := svcWithReview.GetFolderLineage(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFolderLineage(review) error = %v", err)
	}
	if len(resp.Review.ExecutedSteps) == 0 {
		t.Fatalf("review.executed_steps should not be empty")
	}
	hasArtifactNode := false
	hasProducedEdge := false
	for _, node := range resp.Graph.Nodes {
		if node.Type == folderLineageNodeTypeArtifact {
			hasArtifactNode = true
		}
	}
	for _, edge := range resp.Graph.Edges {
		if edge.Type == folderLineageEdgeTypeProduced {
			hasProducedEdge = true
		}
	}
	if !hasArtifactNode || !hasProducedEdge {
		t.Fatalf("review artifact graph not built nodes=%#v edges=%#v", resp.Graph.Nodes, resp.Graph.Edges)
	}

	svcWithFallback := newTestLineageService(
		&repository.Folder{ID: "f2", Path: "/current2", Status: "done", Category: "video"},
		[]*repository.FolderPathObservation{{ID: "o2", FolderID: "f2", Path: "/current2", IsCurrent: true, FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(1)}},
		nil,
		nil,
		nil,
		nil,
		&stubLineageOutputMappingRepo{
			latestWorkflowRunID: "wr-fallback",
			byWorkflowRun: map[string][]*repository.FolderOutputMapping{
				"wr-fallback": {{
					ID:            "m1",
					WorkflowRunID: "wr-fallback",
					FolderID:      "f2",
					OutputPath:    "/out/thumb.jpg",
					NodeType:      "thumbnail-node",
					CreatedAt:     testLineageTime(6),
				}},
			},
		},
	)
	resp, err = svcWithFallback.GetFolderLineage(context.Background(), "f2")
	if err != nil {
		t.Fatalf("GetFolderLineage(fallback) error = %v", err)
	}
	foundFallbackArtifact := false
	for _, node := range resp.Graph.Nodes {
		if node.Type == folderLineageNodeTypeArtifact && node.Path == "/out/thumb.jpg" {
			foundFallbackArtifact = true
			break
		}
	}
	if !foundFallbackArtifact {
		t.Fatalf("fallback artifact node missing: %#v", resp.Graph.Nodes)
	}
}

func TestFolderLineageServiceFailedAuditAddsTimelineWithoutGraphEdge(t *testing.T) {
	t.Parallel()

	svc := newTestLineageService(
		&repository.Folder{ID: "f1", Path: "/to", Status: "pending", Category: "other"},
		[]*repository.FolderPathObservation{
			{ID: "o1", FolderID: "f1", Path: "/from", FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(1)},
			{ID: "o2", FolderID: "f1", Path: "/to", IsCurrent: true, FirstSeenAt: testLineageTime(2), LastSeenAt: testLineageTime(2)},
		},
		[]*repository.Snapshot{{
			ID:        "s1",
			FolderID:  "f1",
			Status:    "committed",
			CreatedAt: testLineageTime(2),
			After: mustJSONRawMessage(t, []map[string]string{{
				"original_path": "/from",
				"current_path":  "/to",
			}}),
		}},
		nil,
		[]*repository.AuditLog{{
			ID:        "a1",
			FolderID:  "f1",
			Action:    "workflow.node.failed",
			Result:    "failed",
			ErrorMsg:  "step failed",
			CreatedAt: testLineageTime(3),
		}},
		nil,
		&stubLineageOutputMappingRepo{},
	)

	resp, err := svc.GetFolderLineage(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFolderLineage() error = %v", err)
	}
	foundFailedEvent := false
	for _, event := range resp.Timeline {
		if event.Type == folderLineageEventTypeProcessingFail {
			foundFailedEvent = true
		}
	}
	if !foundFailedEvent {
		t.Fatalf("processing_failed event missing: %#v", resp.Timeline)
	}
	if len(resp.Graph.Edges) != 1 {
		t.Fatalf("len(graph.edges) = %d, want 1", len(resp.Graph.Edges))
	}
}

func TestFolderLineageServiceBuildsFlowFromLatestManifestAndMappings(t *testing.T) {
	t.Parallel()

	manifests := []*repository.FolderSourceManifest{
		{
			ID:           "sm-2",
			FolderID:     "f1",
			BatchID:      "b1",
			SourcePath:   "/scan/folder/b.png",
			RelativePath: "b.png",
			FileName:     "b.png",
			SizeBytes:    200,
		},
		{
			ID:           "sm-1",
			FolderID:     "f1",
			BatchID:      "b1",
			SourcePath:   "/scan/folder/a.png",
			RelativePath: "a.png",
			FileName:     "a.png",
			SizeBytes:    100,
		},
	}

	svc := newTestLineageService(
		&repository.Folder{ID: "f1", Path: "/scan/folder", SourceDir: "/scan", Status: "done", Category: "mixed"},
		[]*repository.FolderPathObservation{{ID: "o1", FolderID: "f1", Path: "/scan/folder", IsCurrent: true, FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(1)}},
		nil,
		nil,
		nil,
		manifests,
		&stubLineageOutputMappingRepo{
			latestWorkflowRunID: "wr-latest",
			byWorkflowRun: map[string][]*repository.FolderOutputMapping{
				"wr-latest": {
					{
						ID:                 "m1",
						WorkflowRunID:      "wr-latest",
						FolderID:           "f1",
						SourcePath:         "/scan/folder/a.png",
						SourceRelativePath: "a.png",
						OutputPath:         "/target/video/a.mp4",
						NodeType:           "compress-node",
						ArtifactType:       "video",
						CreatedAt:          testLineageTime(2),
					},
					{
						ID:                 "m2",
						WorkflowRunID:      "wr-latest",
						FolderID:           "f1",
						SourcePath:         "/scan/folder/a.png",
						SourceRelativePath: "a.png",
						OutputPath:         "/target/thumb/a.jpg",
						NodeType:           "thumbnail-node",
						ArtifactType:       "image",
						CreatedAt:          testLineageTime(3),
					},
					{
						ID:                 "m3",
						WorkflowRunID:      "wr-latest",
						FolderID:           "f1",
						SourcePath:         "/scan/folder/b.png",
						SourceRelativePath: "b.png",
						OutputPath:         "/target/video/b.mp4",
						NodeType:           "compress-node",
						ArtifactType:       "video",
						CreatedAt:          testLineageTime(4),
					},
				},
			},
		},
	)

	resp, err := svc.GetFolderLineage(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFolderLineage() error = %v", err)
	}
	if resp.Flow == nil {
		t.Fatalf("flow should not be nil")
	}
	if len(resp.Flow.TargetDirectories) != 2 {
		t.Fatalf("len(flow.target_directories) = %d, want 2", len(resp.Flow.TargetDirectories))
	}
	if len(resp.Flow.SourceFiles) != 2 || resp.Flow.SourceFiles[0].RelativePath != "a.png" {
		t.Fatalf("flow.source_files order invalid: %#v", resp.Flow.SourceFiles)
	}
	if len(resp.Flow.TargetFiles) != 3 || len(resp.Flow.Links) != 3 {
		t.Fatalf("target_files/links invalid target=%d links=%d", len(resp.Flow.TargetFiles), len(resp.Flow.Links))
	}
}

func TestFolderLineageServiceFlowIsNilWhenMissingDataOrNoValidLinks(t *testing.T) {
	t.Parallel()

	baseFolder := &repository.Folder{ID: "f1", Path: "/scan/folder", Status: "done", Category: "mixed"}
	baseObservations := []*repository.FolderPathObservation{{
		ID: "o1", FolderID: "f1", Path: "/scan/folder", IsCurrent: true, FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(1),
	}}
	baseMappings := &stubLineageOutputMappingRepo{
		latestWorkflowRunID: "wr-latest",
		byWorkflowRun: map[string][]*repository.FolderOutputMapping{
			"wr-latest": {{
				ID:                 "m1",
				WorkflowRunID:      "wr-latest",
				FolderID:           "f1",
				SourcePath:         "/scan/folder/a.png",
				SourceRelativePath: "a.png",
				OutputPath:         "/target/video/a.mp4",
				NodeType:           "compress-node",
				ArtifactType:       "video",
				CreatedAt:          testLineageTime(2),
			}},
		},
	}

	testCases := []struct {
		name      string
		manifests []*repository.FolderSourceManifest
		mappings  *stubLineageOutputMappingRepo
	}{
		{
			name:      "missing manifests",
			manifests: nil,
			mappings:  baseMappings,
		},
		{
			name: "missing output mappings",
			manifests: []*repository.FolderSourceManifest{{
				ID: "sm-1", FolderID: "f1", BatchID: "b1", SourcePath: "/scan/folder/a.png", RelativePath: "a.png", FileName: "a.png",
			}},
			mappings: &stubLineageOutputMappingRepo{},
		},
		{
			name: "no valid links",
			manifests: []*repository.FolderSourceManifest{{
				ID: "sm-1", FolderID: "f1", BatchID: "b1", SourcePath: "/scan/folder/other.png", RelativePath: "other.png", FileName: "other.png",
			}},
			mappings: baseMappings,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := newTestLineageService(baseFolder, baseObservations, nil, nil, nil, tc.manifests, tc.mappings)
			resp, err := svc.GetFolderLineage(context.Background(), "f1")
			if err != nil {
				t.Fatalf("GetFolderLineage() error = %v", err)
			}
			if resp.Flow != nil {
				t.Fatalf("flow should be nil when %s", tc.name)
			}
		})
	}
}

func TestFolderLineageServiceBuildsFlowFromOlderManifestBatchWhenLatestBatchDoesNotMatch(t *testing.T) {
	t.Parallel()

	manifests := []*repository.FolderSourceManifest{
		{
			ID:           "sm-new-1",
			FolderID:     "f1",
			BatchID:      "b-new",
			SourcePath:   "/processed/folder/a.png",
			RelativePath: "a.png",
			FileName:     "a.png",
			SizeBytes:    100,
		},
		{
			ID:           "sm-old-1",
			FolderID:     "f1",
			BatchID:      "b-old",
			SourcePath:   "/scan/folder/a.png",
			RelativePath: "a.png",
			FileName:     "a.png",
			SizeBytes:    100,
		},
	}

	svc := newTestLineageService(
		&repository.Folder{ID: "f1", Path: "/processed/folder", Status: "done", Category: "mixed"},
		[]*repository.FolderPathObservation{{
			ID: "o1", FolderID: "f1", Path: "/processed/folder", IsCurrent: true, FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(2),
		}},
		nil,
		nil,
		nil,
		manifests,
		&stubLineageOutputMappingRepo{
			latestWorkflowRunID: "wr-latest",
			byWorkflowRun: map[string][]*repository.FolderOutputMapping{
				"wr-latest": {{
					ID:                 "m1",
					WorkflowRunID:      "wr-latest",
					FolderID:           "f1",
					SourcePath:         "/scan/folder/a.png",
					SourceRelativePath: "a.png",
					OutputPath:         "/target/video/a.mp4",
					NodeType:           "compress-node",
					ArtifactType:       "video",
					CreatedAt:          testLineageTime(3),
				}},
			},
		},
	)

	resp, err := svc.GetFolderLineage(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFolderLineage() error = %v", err)
	}
	if resp.Flow == nil {
		t.Fatalf("flow should not be nil")
	}
	if resp.Flow.SourceDirectory.Path != "/scan/folder" {
		t.Fatalf("flow.source_directory.path = %q, want /scan/folder", resp.Flow.SourceDirectory.Path)
	}
	if len(resp.Flow.Links) != 1 {
		t.Fatalf("len(flow.links) = %d, want 1", len(resp.Flow.Links))
	}
}

func TestFolderLineageServiceBuildsFlowWithWindowsDriveLetterCaseDifferences(t *testing.T) {
	t.Parallel()

	manifests := []*repository.FolderSourceManifest{
		{
			ID:           "sm-1",
			FolderID:     "f1",
			BatchID:      "b1",
			SourcePath:   `F:\p\欧美\compilation\reislin\cover.jpg`,
			RelativePath: "cover.jpg",
			FileName:     "cover.jpg",
			SizeBytes:    100,
		},
	}

	svc := newTestLineageService(
		&repository.Folder{ID: "f1", Path: `f:/p/欧美/compilation/reislin`, Status: "done", Category: "photo"},
		[]*repository.FolderPathObservation{{
			ID: "o1", FolderID: "f1", Path: `F:\p\欧美\compilation\reislin`, IsCurrent: true, FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(1),
		}},
		nil,
		nil,
		nil,
		manifests,
		&stubLineageOutputMappingRepo{
			latestWorkflowRunID: "wr-win",
			byWorkflowRun: map[string][]*repository.FolderOutputMapping{
				"wr-win": {{
					ID:                 "m1",
					WorkflowRunID:      "wr-win",
					FolderID:           "f1",
					SourcePath:         `f:/p/欧美/compilation/reislin/cover.jpg`,
					SourceRelativePath: "cover.jpg",
					OutputPath:         `F:\target\thumbs\cover.jpg`,
					NodeType:           "thumbnail-node",
					ArtifactType:       "thumbnail",
					CreatedAt:          testLineageTime(2),
				}},
			},
		},
	)

	resp, err := svc.GetFolderLineage(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFolderLineage() error = %v", err)
	}
	if resp.Flow == nil {
		t.Fatalf("flow should not be nil")
	}
	if resp.Flow.SourceDirectory.Path != "f:/p/欧美/compilation/reislin" {
		t.Fatalf("flow.source_directory.path = %q, want %q", resp.Flow.SourceDirectory.Path, "f:/p/欧美/compilation/reislin")
	}
	if len(resp.Flow.SourceFiles) != 1 || resp.Flow.SourceFiles[0].Path != "f:/p/欧美/compilation/reislin/cover.jpg" {
		t.Fatalf("flow.source_files = %#v, want normalized Windows source path", resp.Flow.SourceFiles)
	}
	if len(resp.Flow.TargetFiles) != 1 || resp.Flow.TargetFiles[0].Path != "f:/target/thumbs/cover.jpg" {
		t.Fatalf("flow.target_files = %#v, want normalized Windows target path", resp.Flow.TargetFiles)
	}
	if len(resp.Flow.Links) != 1 {
		t.Fatalf("len(flow.links) = %d, want 1", len(resp.Flow.Links))
	}
}

func TestFolderLineageServiceSkipsSamePathReviewTransitions(t *testing.T) {
	t.Parallel()

	review := &repository.ProcessingReviewItem{
		ID:            "r-same-path",
		WorkflowRunID: "wr-same-path",
		JobID:         "job-same-path",
		FolderID:      "f1",
		Status:        "approved",
		BeforeJSON: mustJSONRawMessage(t, map[string]any{
			"path": "/same/path",
		}),
		AfterJSON: mustJSONRawMessage(t, map[string]any{
			"path": "/same/path",
		}),
		StepResultsJSON: mustJSONRawMessage(t, []map[string]any{{
			"node_type":   "move-node",
			"source_path": "/same/path",
			"target_path": "/same/path",
			"status":      "skipped",
		}}),
		UpdatedAt: testLineageTime(3),
	}
	svc := newTestLineageService(
		&repository.Folder{ID: "f1", Path: "/same/path", Status: "done", Category: "mixed"},
		[]*repository.FolderPathObservation{{
			ID: "o1", FolderID: "f1", Path: "/same/path", IsCurrent: true, FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(2),
		}},
		nil,
		review,
		nil,
		nil,
		&stubLineageOutputMappingRepo{},
	)

	resp, err := svc.GetFolderLineage(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFolderLineage() error = %v", err)
	}
	for _, edge := range resp.Graph.Edges {
		if edge.Type == folderLineageEdgeTypeMovedTo || edge.Type == folderLineageEdgeTypeRenamedTo {
			t.Fatalf("unexpected path transition edge when from==to: %#v", edge)
		}
	}
	for _, event := range resp.Timeline {
		if event.Type == folderLineageEventTypeMove || event.Type == folderLineageEventTypeRename {
			t.Fatalf("unexpected timeline move/rename event when from==to: %#v", event)
		}
	}
}

func TestFolderLineageServiceReviewTransitionPrefersExecutedSteps(t *testing.T) {
	t.Parallel()

	review := &repository.ProcessingReviewItem{
		ID:            "r-step-first",
		WorkflowRunID: "wr-step-first",
		JobID:         "job-step-first",
		FolderID:      "f1",
		Status:        "approved",
		BeforeJSON: mustJSONRawMessage(t, map[string]any{
			"path": "/wrong/from",
		}),
		AfterJSON: mustJSONRawMessage(t, map[string]any{
			"path": "/wrong/to",
		}),
		StepResultsJSON: mustJSONRawMessage(t, []map[string]any{{
			"node_type":   "move-node",
			"source_path": "/from",
			"target_path": "/to",
			"status":      "moved",
		}}),
		UpdatedAt: testLineageTime(3),
	}
	svc := newTestLineageService(
		&repository.Folder{ID: "f1", Path: "/to", Status: "done", Category: "mixed"},
		[]*repository.FolderPathObservation{
			{ID: "o1", FolderID: "f1", Path: "/from", FirstSeenAt: testLineageTime(1), LastSeenAt: testLineageTime(1)},
			{ID: "o2", FolderID: "f1", Path: "/to", IsCurrent: true, FirstSeenAt: testLineageTime(2), LastSeenAt: testLineageTime(2)},
		},
		nil,
		review,
		nil,
		nil,
		&stubLineageOutputMappingRepo{},
	)

	resp, err := svc.GetFolderLineage(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFolderLineage() error = %v", err)
	}
	foundExpected := false
	for _, event := range resp.Timeline {
		if event.Type != folderLineageEventTypeMove && event.Type != folderLineageEventTypeRename {
			continue
		}
		if event.PathFrom == "/from" && event.PathTo == "/to" {
			foundExpected = true
		}
		if event.PathFrom == "/wrong/from" || event.PathTo == "/wrong/to" {
			t.Fatalf("unexpected fallback before/after transition event: %#v", event)
		}
	}
	if !foundExpected {
		t.Fatalf("expected move event from executed_steps not found, timeline=%#v", resp.Timeline)
	}
}
