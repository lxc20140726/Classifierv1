package service

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/liqiye/classifier/internal/repository"
)

type ProcessingReviewList struct {
	Items   []*repository.ProcessingReviewItem `json:"items"`
	Summary *ProcessingReviewSummary           `json:"summary"`
}

func (s *WorkflowRunnerService) ListProcessingReviews(ctx context.Context, workflowRunID string) (*ProcessingReviewList, error) {
	if s.reviews == nil {
		return &ProcessingReviewList{Items: []*repository.ProcessingReviewItem{}, Summary: &ProcessingReviewSummary{}}, nil
	}

	items, err := s.reviews.ListByWorkflowRunID(ctx, workflowRunID)
	if err != nil {
		return nil, fmt.Errorf("workflowRunner.ListProcessingReviews list review items %q: %w", workflowRunID, err)
	}
	nodeRuns, _, err := s.nodeRuns.List(ctx, repository.NodeRunListFilter{WorkflowRunID: workflowRunID, Page: 1, Limit: 2000})
	if err != nil {
		return nil, fmt.Errorf("workflowRunner.ListProcessingReviews list node runs %q: %w", workflowRunID, err)
	}

	return &ProcessingReviewList{
		Items:   items,
		Summary: buildProcessingReviewSummary(items, nodeRuns),
	}, nil
}

func (s *WorkflowRunnerService) ApproveProcessingReview(ctx context.Context, workflowRunID, reviewID string) error {
	if s.reviews == nil {
		return fmt.Errorf("workflowRunner.ApproveProcessingReview: review repository is not configured")
	}

	run, review, err := s.loadWorkflowRunReview(ctx, workflowRunID, reviewID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(review.Status) != "pending" {
		return fmt.Errorf("workflowRunner.ApproveProcessingReview: review %q status %q is not pending", reviewID, review.Status)
	}

	if err := s.approveProcessingReviewItem(ctx, run, review); err != nil {
		return fmt.Errorf("workflowRunner.ApproveProcessingReview apply decision: %w", err)
	}

	if err := s.refreshReviewDrivenRunStatus(ctx, run.ID); err != nil {
		return fmt.Errorf("workflowRunner.ApproveProcessingReview refresh run status: %w", err)
	}

	return nil
}

func (s *WorkflowRunnerService) RollbackProcessingReview(ctx context.Context, workflowRunID, reviewID string) error {
	if s.reviews == nil {
		return fmt.Errorf("workflowRunner.RollbackProcessingReview: review repository is not configured")
	}

	run, review, err := s.loadWorkflowRunReview(ctx, workflowRunID, reviewID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(review.Status) != "pending" {
		return fmt.Errorf("workflowRunner.RollbackProcessingReview: review %q status %q is not pending", reviewID, review.Status)
	}

	if err := s.rollbackProcessingReviewItem(ctx, run, review); err != nil {
		return fmt.Errorf("workflowRunner.RollbackProcessingReview apply decision: %w", err)
	}

	if err := s.refreshReviewDrivenRunStatus(ctx, run.ID); err != nil {
		return fmt.Errorf("workflowRunner.RollbackProcessingReview refresh run status: %w", err)
	}

	return nil
}

func (s *WorkflowRunnerService) ApproveAllPendingProcessingReviews(ctx context.Context, workflowRunID string) (int, error) {
	if s.reviews == nil {
		return 0, fmt.Errorf("workflowRunner.ApproveAllPendingProcessingReviews: review repository is not configured")
	}

	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return 0, fmt.Errorf("workflowRunner.ApproveAllPendingProcessingReviews get workflow run %q: %w", workflowRunID, err)
	}
	reviews, err := s.reviews.ListByWorkflowRunID(ctx, workflowRunID)
	if err != nil {
		return 0, fmt.Errorf("workflowRunner.ApproveAllPendingProcessingReviews list reviews %q: %w", workflowRunID, err)
	}

	approved := 0
	for _, review := range reviews {
		if review == nil || strings.TrimSpace(review.Status) != "pending" {
			continue
		}
		if err := s.approveProcessingReviewItem(ctx, run, review); err != nil {
			return approved, fmt.Errorf("workflowRunner.ApproveAllPendingProcessingReviews approve review %q: %w", review.ID, err)
		}
		approved++
	}

	if approved == 0 {
		return 0, nil
	}
	if err := s.refreshReviewDrivenRunStatus(ctx, workflowRunID); err != nil {
		return approved, fmt.Errorf("workflowRunner.ApproveAllPendingProcessingReviews refresh run status: %w", err)
	}
	return approved, nil
}

func (s *WorkflowRunnerService) RollbackAllPendingProcessingReviews(ctx context.Context, workflowRunID string) (int, error) {
	if s.reviews == nil {
		return 0, fmt.Errorf("workflowRunner.RollbackAllPendingProcessingReviews: review repository is not configured")
	}

	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return 0, fmt.Errorf("workflowRunner.RollbackAllPendingProcessingReviews get workflow run %q: %w", workflowRunID, err)
	}
	reviews, err := s.reviews.ListByWorkflowRunID(ctx, workflowRunID)
	if err != nil {
		return 0, fmt.Errorf("workflowRunner.RollbackAllPendingProcessingReviews list reviews %q: %w", workflowRunID, err)
	}

	rolledBack := 0
	for _, review := range reviews {
		if review == nil || strings.TrimSpace(review.Status) != "pending" {
			continue
		}
		if err := s.rollbackProcessingReviewItem(ctx, run, review); err != nil {
			return rolledBack, fmt.Errorf("workflowRunner.RollbackAllPendingProcessingReviews rollback review %q: %w", review.ID, err)
		}
		rolledBack++
	}

	if rolledBack == 0 {
		return 0, nil
	}
	if err := s.refreshReviewDrivenRunStatus(ctx, workflowRunID); err != nil {
		return rolledBack, fmt.Errorf("workflowRunner.RollbackAllPendingProcessingReviews refresh run status: %w", err)
	}
	return rolledBack, nil
}

func (s *WorkflowRunnerService) approveProcessingReviewItem(
	ctx context.Context,
	run *repository.WorkflowRun,
	review *repository.ProcessingReviewItem,
) error {
	now := time.Now()
	if err := s.reviews.UpdateDecision(ctx, review.ID, "approved", "", &now); err != nil {
		return fmt.Errorf("update review %q: %w", review.ID, err)
	}
	if err := s.writeProcessingReviewAudit(ctx, run, review, "workflow.processing.review_approved", "success", nil); err != nil {
		return fmt.Errorf("write audit: %w", err)
	}
	return nil
}

func (s *WorkflowRunnerService) rollbackProcessingReviewItem(
	ctx context.Context,
	run *repository.WorkflowRun,
	review *repository.ProcessingReviewItem,
) error {
	if err := s.rollbackWorkflowRunForReview(ctx, run, review); err != nil {
		return fmt.Errorf("rollback review %q: %w", review.ID, err)
	}

	now := time.Now()
	if err := s.reviews.UpdateDecision(ctx, review.ID, "rolled_back", "", &now); err != nil {
		return fmt.Errorf("update review %q: %w", review.ID, err)
	}
	if strings.TrimSpace(review.FolderID) != "" {
		if s.completionSvc != nil {
			if err := s.completionSvc.MarkPending(ctx, review.FolderID); err != nil {
				return fmt.Errorf("mark folder %q pending: %w", review.FolderID, err)
			}
		} else if err := s.folders.UpdateStatus(ctx, review.FolderID, "pending"); err != nil {
			return fmt.Errorf("update folder %q status: %w", review.FolderID, err)
		}
	}
	if err := s.writeProcessingReviewAudit(ctx, run, review, "workflow.processing.review_rolled_back", "success", nil); err != nil {
		return fmt.Errorf("write audit: %w", err)
	}
	return nil
}

func (s *WorkflowRunnerService) prepareProcessingReviews(ctx context.Context, workflowRunID string, runFolder *repository.Folder) (bool, error) {
	if s.reviews == nil {
		return false, nil
	}

	existing, err := s.reviews.ListByWorkflowRunID(ctx, workflowRunID)
	if err != nil {
		return false, fmt.Errorf("list existing review items %q: %w", workflowRunID, err)
	}
	if len(existing) > 0 {
		if err := s.workflowRuns.UpdateStatus(ctx, workflowRunID, "waiting_input", ""); err != nil {
			return false, fmt.Errorf("set waiting_input for existing reviews %q: %w", workflowRunID, err)
		}
		if err := s.syncFolderStatusesByWorkflowRun(ctx, workflowRunID, "waiting_input"); err != nil {
			return false, fmt.Errorf("sync folder statuses for existing reviews run %q: %w", workflowRunID, err)
		}
		s.publishWorkflowRunUpdated(ctx, workflowRunID)
		s.publish("workflow_run.review_pending", map[string]any{
			"workflow_run_id": workflowRunID,
			"total":           len(existing),
			"pending":         countReviewStatus(existing, "pending"),
		})
		return true, nil
	}

	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return false, fmt.Errorf("get workflow run %q: %w", workflowRunID, err)
	}
	nodeRuns, _, err := s.nodeRuns.List(ctx, repository.NodeRunListFilter{WorkflowRunID: workflowRunID, Page: 1, Limit: 2000})
	if err != nil {
		return false, fmt.Errorf("list node runs %q: %w", workflowRunID, err)
	}

	perFolder := map[string]*processingFolderAggregate{}
	folderPathMap := map[string]string{}
	for _, nodeRun := range nodeRuns {
		if nodeRun == nil || strings.TrimSpace(nodeRun.Status) != "succeeded" || strings.TrimSpace(nodeRun.OutputJSON) == "" {
			continue
		}
		typedOutputs, typed, parseErr := parseTypedNodeOutputs(nodeRun.OutputJSON)
		if parseErr != nil || !typed {
			continue
		}

		items, _ := categoryRouterToItems(typedOutputs["items"].Value)
		for _, item := range items {
			folderID := strings.TrimSpace(item.FolderID)
			if folderID == "" {
				continue
			}
			agg := ensureProcessingFolderAggregate(perFolder, folderID)
			sourcePath := processingItemCurrentPath(item)
			if sourcePath != "" {
				agg.SourcePaths[sourcePath] = struct{}{}
				agg.LastPath = sourcePath
				folderPathMap[folderID] = sourcePath
			}
			if strings.TrimSpace(item.TargetName) != "" {
				agg.LastName = strings.TrimSpace(item.TargetName)
			}
		}

		stepResults := processingStepResultsFromNodeRun(nodeRun, typedOutputs)
		if len(stepResults) == 0 {
			continue
		}
		for _, step := range stepResults {
			folderID := ""
			if folderID == "" {
				folderID = resolveFolderIDByStep(step, folderPathMap)
			}
			if folderID == "" && strings.TrimSpace(run.FolderID) != "" {
				folderID = strings.TrimSpace(run.FolderID)
			}
			if folderID == "" {
				continue
			}
			agg := ensureProcessingFolderAggregate(perFolder, folderID)
			agg.StepResults = append(agg.StepResults, step)
			if sourcePath := strings.TrimSpace(step.SourcePath); sourcePath != "" {
				agg.SourcePaths[sourcePath] = struct{}{}
				if agg.BeforePath == "" {
					agg.BeforePath = sourcePath
				}
			}
			if targetPath := strings.TrimSpace(step.TargetPath); targetPath != "" {
				agg.TargetPaths[targetPath] = struct{}{}
				if isProcessingPathTransitionStep(step.NodeType) {
					agg.AfterPath = resolveProcessingStepTargetPath(step.SourcePath, targetPath)
				}
			}
		}
	}

	if len(perFolder) == 0 {
		return false, nil
	}

	folderIDs := make([]string, 0, len(perFolder))
	for folderID := range perFolder {
		if len(perFolder[folderID].StepResults) == 0 {
			continue
		}
		folderIDs = append(folderIDs, folderID)
	}
	if len(folderIDs) == 0 {
		return false, nil
	}
	sort.Strings(folderIDs)

	for _, folderID := range folderIDs {
		agg := perFolder[folderID]
		folder := runFolder
		if strings.TrimSpace(folderID) != "" {
			folder, _ = s.folders.GetByID(ctx, folderID)
		}
		before := buildProcessingReviewBefore(folder, agg)
		after := buildProcessingReviewAfter(folder, agg)
		diff := buildProcessingReviewDiff(before, after, agg.StepResults)
		stepResults := dedupeProcessingStepResults(agg.StepResults)

		beforeJSON, err := json.Marshal(before)
		if err != nil {
			return false, fmt.Errorf("marshal review before for folder %q: %w", folderID, err)
		}
		afterJSON, err := json.Marshal(after)
		if err != nil {
			return false, fmt.Errorf("marshal review after for folder %q: %w", folderID, err)
		}
		diffJSON, err := json.Marshal(diff)
		if err != nil {
			return false, fmt.Errorf("marshal review diff for folder %q: %w", folderID, err)
		}
		stepResultsJSON, err := json.Marshal(stepResults)
		if err != nil {
			return false, fmt.Errorf("marshal review step results for folder %q: %w", folderID, err)
		}

		item := &repository.ProcessingReviewItem{
			ID:              uuid.NewString(),
			WorkflowRunID:   run.ID,
			JobID:           run.JobID,
			FolderID:        folderID,
			Status:          "pending",
			BeforeJSON:      beforeJSON,
			AfterJSON:       afterJSON,
			StepResultsJSON: stepResultsJSON,
			DiffJSON:        diffJSON,
		}
		if err := s.reviews.Create(ctx, item); err != nil {
			return false, fmt.Errorf("create review item for folder %q: %w", folderID, err)
		}
	}

	if err := s.workflowRuns.UpdateStatus(ctx, run.ID, "waiting_input", ""); err != nil {
		return false, fmt.Errorf("set waiting_input for review run %q: %w", run.ID, err)
	}
	if err := s.syncFolderStatusesByWorkflowRun(ctx, run.ID, "waiting_input"); err != nil {
		return false, fmt.Errorf("sync folder statuses for review run %q: %w", run.ID, err)
	}
	s.publishWorkflowRunUpdated(ctx, run.ID)

	items, err := s.reviews.ListByWorkflowRunID(ctx, run.ID)
	if err != nil {
		return false, fmt.Errorf("list created review items for run %q: %w", run.ID, err)
	}
	s.publish("workflow_run.review_pending", map[string]any{
		"job_id":          run.JobID,
		"workflow_run_id": run.ID,
		"total":           len(items),
		"pending":         countReviewStatus(items, "pending"),
	})

	return true, nil
}

func (s *WorkflowRunnerService) refreshReviewDrivenRunStatus(ctx context.Context, workflowRunID string) error {
	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return fmt.Errorf("get workflow run %q: %w", workflowRunID, err)
	}
	items, err := s.reviews.ListByWorkflowRunID(ctx, workflowRunID)
	if err != nil {
		return fmt.Errorf("list review items for run %q: %w", workflowRunID, err)
	}
	nodeRuns, _, err := s.nodeRuns.List(ctx, repository.NodeRunListFilter{WorkflowRunID: workflowRunID, Page: 1, Limit: 2000})
	if err != nil {
		return fmt.Errorf("list node runs for run %q: %w", workflowRunID, err)
	}

	summary := buildProcessingReviewSummary(items, nodeRuns)
	nextStatus := "waiting_input"
	if summary.Total == 0 {
		nextStatus = "succeeded"
	} else if summary.Pending > 0 {
		nextStatus = "waiting_input"
	} else if summary.Approved == summary.Total {
		nextStatus = "succeeded"
	} else if summary.RolledBack == summary.Total {
		nextStatus = "rolled_back"
	} else {
		nextStatus = "partial"
	}

	if err := s.workflowRuns.UpdateStatus(ctx, run.ID, nextStatus, ""); err != nil {
		return fmt.Errorf("update workflow run %q status %q: %w", run.ID, nextStatus, err)
	}
	if err := s.syncFolderStatusesByWorkflowRun(ctx, run.ID, nextStatus); err != nil {
		return fmt.Errorf("sync folder statuses for workflow run %q status %q: %w", run.ID, nextStatus, err)
	}
	s.publishWorkflowRunUpdated(ctx, run.ID)

	if nextStatus == "waiting_input" {
		if err := s.jobs.UpdateStatus(ctx, run.JobID, "waiting_input", ""); err != nil {
			return fmt.Errorf("update job %q waiting_input: %w", run.JobID, err)
		}
	} else {
		job, jobErr := s.jobs.GetByID(ctx, run.JobID)
		if jobErr != nil {
			return fmt.Errorf("get job %q: %w", run.JobID, jobErr)
		}
		if strings.TrimSpace(job.Status) == "waiting_input" {
			if err := s.jobs.IncrementProgress(ctx, run.JobID, 1, 0); err != nil {
				return fmt.Errorf("increment job %q progress: %w", run.JobID, err)
			}
		}
		if err := s.jobs.UpdateStatus(ctx, run.JobID, nextStatus, ""); err != nil {
			return fmt.Errorf("update job %q final status %q: %w", run.JobID, nextStatus, err)
		}
	}

	s.publish("workflow_run.review_updated", map[string]any{
		"job_id":          run.JobID,
		"workflow_run_id": run.ID,
		"status":          nextStatus,
		"summary":         summary,
	})

	return nil
}

func (s *WorkflowRunnerService) rollbackWorkflowRunForReview(ctx context.Context, run *repository.WorkflowRun, review *repository.ProcessingReviewItem) error {
	nodeRuns, err := s.listSucceededNodeRunsDesc(ctx, run.ID)
	if err != nil {
		return err
	}

	var folder *repository.Folder
	if strings.TrimSpace(review.FolderID) != "" {
		folder, _ = s.folders.GetByID(ctx, review.FolderID)
	}

	for _, nodeRun := range nodeRuns {
		executor, ok := s.executors[nodeRun.NodeType]
		if !ok {
			continue
		}
		snaps, snapErr := s.nodeSnapshots.ListByNodeRunID(ctx, nodeRun.ID)
		if snapErr != nil {
			return fmt.Errorf("list snapshots for node run %q: %w", nodeRun.ID, snapErr)
		}

		if rbErr := executor.Rollback(ctx, NodeRollbackInput{
			WorkflowRun: run,
			NodeRun:     nodeRun,
			Snapshots:   snaps,
			Folder:      folder,
		}); rbErr != nil {
			return fmt.Errorf("rollback node %q for review %q: %w", nodeRun.NodeID, review.ID, rbErr)
		}
	}

	return nil
}

func (s *WorkflowRunnerService) listSucceededNodeRunsDesc(ctx context.Context, workflowRunID string) ([]*repository.NodeRun, error) {
	nodeRuns, _, err := s.nodeRuns.List(ctx, repository.NodeRunListFilter{WorkflowRunID: workflowRunID, Page: 1, Limit: 2000})
	if err != nil {
		return nil, fmt.Errorf("list node runs for workflow run %q: %w", workflowRunID, err)
	}
	sort.Slice(nodeRuns, func(i, j int) bool {
		return nodeRuns[i].Sequence > nodeRuns[j].Sequence
	})
	out := make([]*repository.NodeRun, 0, len(nodeRuns))
	for _, nodeRun := range nodeRuns {
		if nodeRun == nil || strings.TrimSpace(nodeRun.Status) != "succeeded" {
			continue
		}
		out = append(out, nodeRun)
	}
	return out, nil
}

func (s *WorkflowRunnerService) loadWorkflowRunReview(ctx context.Context, workflowRunID, reviewID string) (*repository.WorkflowRun, *repository.ProcessingReviewItem, error) {
	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return nil, nil, fmt.Errorf("get workflow run %q: %w", workflowRunID, err)
	}
	review, err := s.reviews.GetByID(ctx, reviewID)
	if err != nil {
		return nil, nil, fmt.Errorf("get review item %q: %w", reviewID, err)
	}
	if review.WorkflowRunID != run.ID {
		return nil, nil, fmt.Errorf("review item %q does not belong to workflow run %q", review.ID, run.ID)
	}
	return run, review, nil
}

func (s *WorkflowRunnerService) writeProcessingReviewAudit(
	ctx context.Context,
	run *repository.WorkflowRun,
	review *repository.ProcessingReviewItem,
	action string,
	result string,
	auditErr error,
) error {
	if s.auditSvc == nil {
		return nil
	}

	detail := map[string]any{
		"workflow_run_id":      run.ID,
		"review_item_id":       review.ID,
		"folder_id":            review.FolderID,
		"before":               jsonRawToObject(review.BeforeJSON),
		"after":                jsonRawToObject(review.AfterJSON),
		"diff":                 jsonRawToObject(review.DiffJSON),
		"step_results_summary": summarizeStepResults(review.StepResultsJSON),
	}
	if auditErr != nil {
		detail["error"] = auditErr.Error()
	}
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("marshal review audit detail: %w", err)
	}

	return s.auditSvc.Write(ctx, &repository.AuditLog{
		JobID:      run.JobID,
		FolderID:   review.FolderID,
		FolderPath: extractPathFromRaw(review.AfterJSON),
		Action:     action,
		Result:     result,
		Detail:     detailJSON,
		ErrorMsg:   errorString(auditErr),
	})
}

func buildProcessingReviewSummary(items []*repository.ProcessingReviewItem, nodeRuns []*repository.NodeRun) *ProcessingReviewSummary {
	summary := &ProcessingReviewSummary{
		Total: len(items),
	}
	for _, item := range items {
		switch strings.TrimSpace(item.Status) {
		case "pending":
			summary.Pending++
		case "approved":
			summary.Approved++
		case "rolled_back":
			summary.RolledBack++
		case "rejected":
			summary.Rejected++
		}
	}
	for _, nodeRun := range nodeRuns {
		if strings.TrimSpace(nodeRun.Status) == "failed" {
			summary.FailedStepRuns++
		}
	}
	return summary
}

type processingFolderAggregate struct {
	BeforePath  string
	AfterPath   string
	LastPath    string
	LastName    string
	SourcePaths map[string]struct{}
	TargetPaths map[string]struct{}
	StepResults []ProcessingStepResult
}

func ensureProcessingFolderAggregate(target map[string]*processingFolderAggregate, folderID string) *processingFolderAggregate {
	agg, ok := target[folderID]
	if ok {
		return agg
	}
	agg = &processingFolderAggregate{
		SourcePaths: map[string]struct{}{},
		TargetPaths: map[string]struct{}{},
		StepResults: make([]ProcessingStepResult, 0, 8),
	}
	target[folderID] = agg
	return agg
}

type reviewNodeRunInputPayload struct {
	Node struct {
		Label  string         `json:"label"`
		Config map[string]any `json:"config"`
	} `json:"node"`
	Inputs    map[string]any        `json:"inputs"`
	AppConfig *repository.AppConfig `json:"app_config"`
}

func processingStepResultsFromNodeRun(nodeRun *repository.NodeRun, typedOutputs map[string]TypedValue) []ProcessingStepResult {
	if persisted, err := extractPersistedProcessingStepResults(nodeRun); err == nil {
		return persisted
	}

	items, _ := categoryRouterToItems(typedOutputs["items"].Value)
	fallbackSteps := processingStepResultsFromAny(typedOutputs["step_results"].Value)
	nodeType := strings.TrimSpace(nodeRun.NodeType)
	fallbackByNodeType := make([]ProcessingStepResult, 0, len(fallbackSteps))
	for _, step := range fallbackSteps {
		if strings.TrimSpace(step.NodeType) != nodeType {
			continue
		}
		fallbackByNodeType = append(fallbackByNodeType, step)
	}
	if len(items) == 0 {
		return fallbackByNodeType
	}

	inputPayload := parseReviewNodeRunInputPayload(nodeRun)
	nodeLabel := strings.TrimSpace(inputPayload.Node.Label)
	if nodeLabel == "" {
		nodeLabel = strings.TrimSpace(nodeRun.NodeID)
	}

	switch nodeType {
	case renameNodeExecutorType:
		out := make([]ProcessingStepResult, 0, len(items))
		for _, item := range items {
			sourcePath := processingItemCurrentPath(item)
			targetName := strings.TrimSpace(item.TargetName)
			status := "renamed"
			if targetName == "" || targetName == filepath.Base(sourcePath) {
				status = "skipped"
			}
			out = append(out, ProcessingStepResult{
				SourcePath: sourcePath,
				TargetPath: resolveProcessingStepTargetPath(sourcePath, targetName),
				NodeType:   renameNodeExecutorType,
				NodeLabel:  nodeLabel,
				Status:     status,
			})
		}
		return out
	case phase4MoveNodeExecutorType:
		inputItems, _ := categoryRouterToItems(inputPayload.Inputs["items"])
		inputByFolderID := map[string]ProcessingItem{}
		inputByPath := map[string]ProcessingItem{}
		for _, item := range inputItems {
			if folderID := strings.TrimSpace(item.FolderID); folderID != "" {
				inputByFolderID[folderID] = item
			}
			if sourcePath := processingItemCurrentPath(item); sourcePath != "" {
				inputByPath[sourcePath] = item
			}
		}

		out := make([]ProcessingStepResult, 0, len(items))
		for _, item := range items {
			targetPath := processingItemCurrentPath(item)
			sourcePath := targetPath
			if folderID := strings.TrimSpace(item.FolderID); folderID != "" {
				if before, ok := inputByFolderID[folderID]; ok {
					sourcePath = processingItemCurrentPath(before)
				}
			}
			if sourcePath == targetPath {
				if before, ok := inputByPath[targetPath]; ok {
					sourcePath = processingItemCurrentPath(before)
				}
			}
			status := "moved"
			if sourcePath == targetPath {
				status = "skipped"
			}
			out = append(out, ProcessingStepResult{
				SourcePath: sourcePath,
				TargetPath: targetPath,
				NodeType:   phase4MoveNodeExecutorType,
				NodeLabel:  nodeLabel,
				Status:     status,
			})
		}
		return out
	case compressNodeExecutorType:
		cfg := inputPayload.Node.Config
		format := strings.ToLower(strings.TrimSpace(stringConfig(cfg, "format")))
		if format == "" {
			format = "cbz"
		}
		if format != "cbz" && format != "zip" {
			format = "cbz"
		}
		archiveDir := resolveWorkflowNodePath(cfg, inputPayload.AppConfig, workflowNodePathOptions{
			DefaultType:      workflowPathRefTypeOutput,
			DefaultOutputKey: "mixed",
			LegacyKeys:       []string{"target_dir", "output_dir"},
		})
		if archiveDir == "" {
			archiveDir = ".archives"
		}
		archiveDir = normalizeWorkflowPath(archiveDir)

		out := make([]ProcessingStepResult, 0, len(items))
		for _, item := range items {
			sourcePath := processingItemCurrentPath(item)
			name := processingItemArtifactName(item)
			if name == "" {
				name = strings.TrimSpace(filepath.Base(sourcePath))
			}
			targetPath := normalizeWorkflowPath(joinWorkflowPath(archiveDir, name+"."+format))
			out = append(out, ProcessingStepResult{
				SourcePath: sourcePath,
				TargetPath: targetPath,
				NodeType:   compressNodeExecutorType,
				NodeLabel:  nodeLabel,
				Status:     "succeeded",
			})
		}
		return out
	case thumbnailNodeExecutorType:
		cfg := inputPayload.Node.Config
		configuredOutputDir := resolveWorkflowNodePath(cfg, inputPayload.AppConfig, workflowNodePathOptions{
			DefaultType:      workflowPathRefTypeOutput,
			DefaultOutputKey: "video",
			LegacyKeys:       []string{"output_dir", "target_dir"},
		})

		out := make([]ProcessingStepResult, 0, len(items))
		for _, item := range items {
			sourcePath := processingItemCurrentPath(item)
			outputDir := configuredOutputDir
			if outputDir == "" {
				outputDir = normalizeWorkflowPath(thumbnailNodeDefaultOutputDir(item))
			}
			outputName := processingItemArtifactName(item)
			if outputName == "" {
				outputName = thumbnailNodeOutputBaseName(item)
			}
			targetPath := normalizeWorkflowPath(joinWorkflowPath(outputDir, outputName+".jpg"))
			out = append(out, ProcessingStepResult{
				SourcePath: sourcePath,
				TargetPath: targetPath,
				NodeType:   thumbnailNodeExecutorType,
				NodeLabel:  nodeLabel,
				Status:     "succeeded",
			})
		}
		return out
	default:
		return fallbackByNodeType
	}
}

func parseReviewNodeRunInputPayload(nodeRun *repository.NodeRun) reviewNodeRunInputPayload {
	payload := reviewNodeRunInputPayload{}
	if nodeRun == nil || strings.TrimSpace(nodeRun.InputJSON) == "" {
		return payload
	}
	_ = json.Unmarshal([]byte(nodeRun.InputJSON), &payload)
	if payload.Inputs == nil {
		payload.Inputs = map[string]any{}
	}
	if payload.Node.Config == nil {
		payload.Node.Config = map[string]any{}
	}
	return payload
}

func resolveFolderIDByStep(step ProcessingStepResult, folderPathMap map[string]string) string {
	sourcePath := strings.TrimSpace(step.SourcePath)
	targetPath := strings.TrimSpace(step.TargetPath)
	for folderID, knownPath := range folderPathMap {
		if knownPath == "" {
			continue
		}
		if sourcePath != "" && (sourcePath == knownPath || strings.HasPrefix(sourcePath, knownPath+string(filepath.Separator))) {
			return folderID
		}
		if targetPath != "" && (targetPath == knownPath || strings.HasPrefix(targetPath, knownPath+string(filepath.Separator))) {
			return folderID
		}
	}
	return ""
}

func buildProcessingReviewBefore(folder *repository.Folder, agg *processingFolderAggregate) map[string]any {
	path := processingReviewDisplayPath(folder, agg, false)
	name := filepath.Base(path)
	if strings.TrimSpace(name) == "." && folder != nil {
		name = strings.TrimSpace(folder.Name)
	}
	return map[string]any{
		"path":            path,
		"name":            strings.TrimSpace(name),
		"cover_image":     folderCover(folder),
		"status":          folderStatus(folder, "pending"),
		"key_files_count": folderFileCount(folder),
	}
}

func buildProcessingReviewAfter(folder *repository.Folder, agg *processingFolderAggregate) map[string]any {
	path := processingReviewDisplayPath(folder, agg, true)
	name := filepath.Base(path)
	if strings.TrimSpace(name) == "." && folder != nil {
		name = strings.TrimSpace(folder.Name)
	}
	artifacts := collectArtifactTargets(agg.StepResults)
	return map[string]any{
		"path":           path,
		"name":           strings.TrimSpace(name),
		"cover_image":    folderCover(folder),
		"status":         folderStatus(folder, "pending"),
		"artifact_paths": artifacts,
	}
}

func processingReviewDisplayPath(folder *repository.Folder, agg *processingFolderAggregate, preferAfter bool) string {
	candidates := make([]string, 0, 12)
	if agg != nil {
		if preferAfter {
			candidates = append(candidates, strings.TrimSpace(agg.AfterPath))
			candidates = append(candidates, strings.TrimSpace(agg.LastPath))
			candidates = append(candidates, sortedPathKeys(agg.TargetPaths)...)
			candidates = append(candidates, sortedPathKeys(agg.SourcePaths)...)
			candidates = append(candidates, strings.TrimSpace(agg.BeforePath))
		} else {
			candidates = append(candidates, strings.TrimSpace(agg.BeforePath))
			candidates = append(candidates, sortedPathKeys(agg.SourcePaths)...)
			candidates = append(candidates, strings.TrimSpace(agg.LastPath))
			candidates = append(candidates, strings.TrimSpace(agg.AfterPath))
			candidates = append(candidates, sortedPathKeys(agg.TargetPaths)...)
		}
	}

	if folder != nil {
		candidates = append(candidates, strings.TrimSpace(folder.Path))
	}

	for _, candidate := range candidates {
		if candidate != "" {
			return candidate
		}
	}
	return ""
}

func buildProcessingReviewDiff(before map[string]any, after map[string]any, stepResults []ProcessingStepResult) map[string]any {
	beforePath := strings.TrimSpace(anyString(before["path"]))
	afterPath := strings.TrimSpace(anyString(after["path"]))
	beforeName := strings.TrimSpace(anyString(before["name"]))
	afterName := strings.TrimSpace(anyString(after["name"]))
	return map[string]any{
		"path_changed":   isProcessingReviewPathChanged(beforePath, afterPath),
		"name_changed":   beforeName != "" && afterName != "" && beforeName != afterName,
		"new_artifacts":  collectArtifactTargets(stepResults),
		"executed_steps": summarizeSteps(stepResults),
	}
}

func dedupeProcessingStepResults(items []ProcessingStepResult) []ProcessingStepResult {
	seen := map[string]struct{}{}
	out := make([]ProcessingStepResult, 0, len(items))
	for _, item := range items {
		key := strings.Join([]string{
			strings.TrimSpace(item.FolderID),
			strings.TrimSpace(item.SourcePath),
			strings.TrimSpace(item.TargetPath),
			strings.TrimSpace(item.NodeType),
			strings.TrimSpace(item.Status),
			strings.TrimSpace(item.Error),
		}, "|")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func summarizeSteps(results []ProcessingStepResult) []map[string]string {
	stepSet := map[string]struct{}{}
	out := make([]map[string]string, 0, len(results))
	for _, step := range results {
		nodeType := strings.TrimSpace(step.NodeType)
		if nodeType == "" {
			continue
		}
		sourcePath := strings.TrimSpace(step.SourcePath)
		targetPath := resolveProcessingStepTargetPath(step.SourcePath, step.TargetPath)
		key := strings.Join([]string{
			nodeType,
			strings.TrimSpace(step.NodeLabel),
			strings.TrimSpace(step.Status),
			sourcePath,
			targetPath,
		}, "|")
		if _, ok := stepSet[key]; ok {
			continue
		}
		stepSet[key] = struct{}{}
		out = append(out, map[string]string{
			"node_type":   nodeType,
			"node_label":  strings.TrimSpace(step.NodeLabel),
			"status":      strings.TrimSpace(step.Status),
			"source_path": sourcePath,
			"target_path": targetPath,
		})
	}
	return out
}

func isProcessingPathTransitionStep(nodeType string) bool {
	switch strings.TrimSpace(nodeType) {
	case renameNodeExecutorType, phase4MoveNodeExecutorType:
		return true
	default:
		return false
	}
}

func resolveProcessingStepTargetPath(sourcePath string, targetPath string) string {
	trimmedTarget := strings.TrimSpace(targetPath)
	if trimmedTarget == "" {
		return ""
	}
	if filepath.IsAbs(trimmedTarget) || strings.ContainsAny(trimmedTarget, `/\`) {
		return strings.TrimSpace(trimmedTarget)
	}
	trimmedSource := strings.TrimSpace(sourcePath)
	if trimmedSource == "" {
		return trimmedTarget
	}
	return strings.TrimSpace(filepath.Join(filepath.Dir(trimmedSource), trimmedTarget))
}

func sortedPathKeys(items map[string]struct{}) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for path := range items {
		trimmed := strings.TrimSpace(path)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out
}

func normalizeProcessingReviewComparePath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}
	cleaned := filepath.Clean(trimmed)
	cleaned = strings.ReplaceAll(cleaned, `\`, "/")
	return normalizeWindowsDriveLetter(cleaned)
}

func isProcessingReviewPathChanged(beforePath, afterPath string) bool {
	beforeNormalized := normalizeProcessingReviewComparePath(beforePath)
	afterNormalized := normalizeProcessingReviewComparePath(afterPath)
	return beforeNormalized != "" && afterNormalized != "" && beforeNormalized != afterNormalized
}

func collectArtifactTargets(results []ProcessingStepResult) []string {
	targets := make([]string, 0, len(results))
	for _, step := range results {
		nodeType := strings.TrimSpace(step.NodeType)
		if nodeType != compressNodeExecutorType && nodeType != thumbnailNodeExecutorType {
			continue
		}
		target := strings.TrimSpace(step.TargetPath)
		if target == "" {
			continue
		}
		targets = append(targets, target)
	}
	return uniqueCompactStringSlice(targets)
}

func countReviewStatus(items []*repository.ProcessingReviewItem, status string) int {
	count := 0
	for _, item := range items {
		if strings.EqualFold(strings.TrimSpace(item.Status), strings.TrimSpace(status)) {
			count++
		}
	}
	return count
}

func jsonRawToObject(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func summarizeStepResults(raw json.RawMessage) map[string]any {
	results := processingStepResultsFromAny(jsonRawToObject(raw))
	return map[string]any{
		"total":      len(results),
		"node_types": summarizeStepNodeTypes(results),
	}
}

func summarizeStepNodeTypes(results []ProcessingStepResult) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(results))
	for _, step := range results {
		nodeType := strings.TrimSpace(step.NodeType)
		if nodeType == "" {
			continue
		}
		if _, ok := seen[nodeType]; ok {
			continue
		}
		seen[nodeType] = struct{}{}
		out = append(out, nodeType)
	}
	sort.Strings(out)
	return out
}

func extractPathFromRaw(raw json.RawMessage) string {
	value := jsonRawToObject(raw)
	m, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	return strings.TrimSpace(anyString(m["path"]))
}

func folderCover(folder *repository.Folder) string {
	if folder == nil {
		return ""
	}
	return strings.TrimSpace(folder.CoverImagePath)
}

func folderStatus(folder *repository.Folder, fallback string) string {
	if folder == nil {
		return fallback
	}
	status := strings.TrimSpace(folder.Status)
	if status == "" {
		return fallback
	}
	return status
}

func folderFileCount(folder *repository.Folder) int {
	if folder == nil {
		return 0
	}
	return folder.TotalFiles
}
