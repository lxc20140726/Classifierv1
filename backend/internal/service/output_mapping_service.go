package service

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	"github.com/liqiye/classifier/internal/repository"
)

type OutputMappingService struct {
	workflowRuns repository.WorkflowRunRepository
	nodeRuns     repository.NodeRunRepository
	folders      repository.FolderRepository
	mappings     repository.OutputMappingRepository
}

func NewOutputMappingService(
	workflowRunRepo repository.WorkflowRunRepository,
	nodeRunRepo repository.NodeRunRepository,
	folderRepo repository.FolderRepository,
	mappingRepo repository.OutputMappingRepository,
) *OutputMappingService {
	return &OutputMappingService{
		workflowRuns: workflowRunRepo,
		nodeRuns:     nodeRunRepo,
		folders:      folderRepo,
		mappings:     mappingRepo,
	}
}

func (s *OutputMappingService) Build(ctx context.Context, workflowRunID string) error {
	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return fmt.Errorf("outputMapping.Build get workflow run %q: %w", workflowRunID, err)
	}

	nodeRuns, _, err := s.nodeRuns.List(ctx, repository.NodeRunListFilter{
		WorkflowRunID: workflowRunID,
		Page:          1,
		Limit:         2000,
	})
	if err != nil {
		return fmt.Errorf("outputMapping.Build list node runs for workflow run %q: %w", workflowRunID, err)
	}

	collected := make([]*repository.FolderOutputMapping, 0, 128)
	seen := make(map[string]struct{}, 128)
	for _, nodeRun := range nodeRuns {
		if nodeRun == nil || strings.TrimSpace(nodeRun.Status) != "succeeded" {
			continue
		}

		nodeType := strings.TrimSpace(nodeRun.NodeType)
		if _, ok := processingStatusDrivenNodeTypes[nodeType]; !ok {
			continue
		}

		stepResults, err := extractPersistedProcessingStepResults(nodeRun)
		if err != nil {
			return fmt.Errorf("output_protocol_invalid: node run %q parse persisted step_results: %w", nodeRun.ID, err)
		}

		typedOutputs, typed, parseErr := parseTypedNodeOutputs(nodeRun.OutputJSON)
		if parseErr != nil {
			return fmt.Errorf("output_protocol_invalid: node run %q parse output json: %w", nodeRun.ID, parseErr)
		}
		if !typed {
			return fmt.Errorf("output_protocol_invalid: node run %q output is not typed payload", nodeRun.ID)
		}

		items, _ := categoryRouterToItems(typedOutputs["items"].Value)
		folderPathMap := outputMappingFolderPathMap(items)

		for _, step := range stepResults {
			if err := outputMappingValidateStepContract(nodeRun, step); err != nil {
				return err
			}

			targetPath := normalizeWorkflowPath(step.TargetPath)
			sourcePath := normalizeWorkflowPath(step.SourcePath)
			folderID := outputMappingResolveFolderID(items, step, folderPathMap)
			if folderID == "" {
				return fmt.Errorf("output_protocol_invalid: node run %q step result has no resolvable folder_id (source_path=%q,target_path=%q)", nodeRun.ID, step.SourcePath, step.TargetPath)
			}

			artifactType, requiredArtifact := outputMappingArtifactType(step.NodeType)
			sourceRelativePath := outputMappingSourceRelativePath(ctx, s.folders, folderID, sourcePath)
			outputContainer := normalizeWorkflowPath(filepath.Dir(targetPath))

			key := strings.Join([]string{
				folderID,
				sourcePath,
				targetPath,
				strings.TrimSpace(step.NodeType),
				artifactType,
			}, "|")
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}

			collected = append(collected, &repository.FolderOutputMapping{
				ID:                 uuid.NewString(),
				WorkflowRunID:      run.ID,
				FolderID:           folderID,
				SourcePath:         sourcePath,
				SourceRelativePath: sourceRelativePath,
				OutputPath:         targetPath,
				OutputContainer:    outputContainer,
				NodeType:           strings.TrimSpace(step.NodeType),
				ArtifactType:       artifactType,
				RequiredArtifact:   requiredArtifact,
			})
		}
	}

	if err := s.mappings.ReplaceByWorkflowRunID(ctx, run.ID, collected); err != nil {
		return fmt.Errorf("outputMapping.Build replace mappings for workflow run %q: %w", workflowRunID, err)
	}

	return nil
}

func outputMappingArtifactType(nodeType string) (string, bool) {
	switch strings.TrimSpace(nodeType) {
	case compressNodeExecutorType:
		return "archive", true
	case thumbnailNodeExecutorType:
		return "thumbnail", true
	case phase4MoveNodeExecutorType:
		return "primary", false
	case renameNodeExecutorType:
		return "rename", false
	default:
		return "primary", false
	}
}

func outputMappingResolveFolderID(
	items []ProcessingItem,
	step ProcessingStepResult,
	folderPathMap map[string]string,
) string {
	if folderID := strings.TrimSpace(step.FolderID); folderID != "" {
		return folderID
	}

	for _, item := range items {
		folderID := strings.TrimSpace(item.FolderID)
		if folderID == "" {
			continue
		}
		candidates := []string{
			normalizeWorkflowPath(item.SourcePath),
			normalizeWorkflowPath(item.CurrentPath),
			normalizeWorkflowPath(item.OriginalSourcePath),
		}
		for _, candidate := range candidates {
			if candidate == "" {
				continue
			}
			sourcePath := normalizeWorkflowPath(step.SourcePath)
			targetPath := normalizeWorkflowPath(step.TargetPath)
			if sourcePath != "" && (sourcePath == candidate || strings.HasPrefix(sourcePath, candidate+string(filepath.Separator))) {
				return folderID
			}
			if targetPath != "" && (targetPath == candidate || strings.HasPrefix(targetPath, candidate+string(filepath.Separator))) {
				return folderID
			}
		}
	}

	folderID := resolveFolderIDByStep(step, folderPathMap)
	if strings.TrimSpace(folderID) != "" {
		return strings.TrimSpace(folderID)
	}

	return ""
}

func outputMappingValidateStepContract(nodeRun *repository.NodeRun, step ProcessingStepResult) error {
	nodeType := strings.TrimSpace(step.NodeType)
	if nodeType == "" {
		nodeType = strings.TrimSpace(nodeRun.NodeType)
	}
	if nodeType == "" {
		return fmt.Errorf("output_protocol_invalid: node run %q step result missing node_type", nodeRun.ID)
	}
	if strings.TrimSpace(step.FolderID) == "" {
		return fmt.Errorf("output_protocol_invalid: node run %q step result missing folder_id", nodeRun.ID)
	}

	switch nodeType {
	case renameNodeExecutorType, phase4MoveNodeExecutorType, compressNodeExecutorType, thumbnailNodeExecutorType:
		if strings.TrimSpace(step.SourcePath) == "" {
			return fmt.Errorf("output_protocol_invalid: node run %q (%s) step result missing source_path", nodeRun.ID, nodeType)
		}
		if strings.TrimSpace(step.TargetPath) == "" {
			return fmt.Errorf("output_protocol_invalid: node run %q (%s) step result missing target_path", nodeRun.ID, nodeType)
		}
	}

	return nil
}

func outputMappingFolderPathMap(items []ProcessingItem) map[string]string {
	out := map[string]string{}
	for _, item := range items {
		folderID := strings.TrimSpace(item.FolderID)
		if folderID == "" {
			continue
		}
		currentPath := normalizeWorkflowPath(processingItemCurrentPath(item))
		if currentPath != "" {
			out[folderID] = currentPath
			continue
		}
		sourcePath := normalizeWorkflowPath(item.SourcePath)
		if sourcePath != "" {
			out[folderID] = sourcePath
		}
	}
	return out
}

func outputMappingSourceRelativePath(ctx context.Context, folders repository.FolderRepository, folderID, sourcePath string) string {
	if folders == nil {
		return ""
	}
	folder, err := folders.GetByID(ctx, folderID)
	if err != nil || folder == nil {
		return ""
	}

	rootPath := normalizeWorkflowPath(folder.Path)
	normalizedSourcePath := normalizeWorkflowPath(sourcePath)
	if rootPath == "" || normalizedSourcePath == "" {
		return ""
	}
	if normalizedSourcePath == rootPath {
		return ""
	}
	if !strings.HasPrefix(normalizedSourcePath, rootPath+string(filepath.Separator)) {
		return ""
	}
	relativePath, err := filepath.Rel(rootPath, normalizedSourcePath)
	if err != nil {
		return ""
	}

	return normalizeWorkflowPath(relativePath)
}
