package service

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/liqiye/classifier/internal/repository"
)

func buildPathAccumulators(observations []*repository.FolderPathObservation, currentPath string) map[string]*folderLineagePathAccumulator {
	out := make(map[string]*folderLineagePathAccumulator, len(observations)+1)
	for _, observation := range observations {
		if observation == nil {
			continue
		}
		path := normalizeLineagePath(observation.Path)
		if path == "" {
			continue
		}
		acc := ensurePathAccumulator(out, path)
		if !observation.FirstSeenAt.IsZero() {
			if !acc.hasFirstSeen || observation.FirstSeenAt.Before(acc.firstSeenAt) {
				acc.firstSeenAt = observation.FirstSeenAt
				acc.hasFirstSeen = true
			}
		}
		if !observation.LastSeenAt.IsZero() {
			if !acc.hasLastSeen || observation.LastSeenAt.After(acc.lastSeenAt) {
				acc.lastSeenAt = observation.LastSeenAt
				acc.hasLastSeen = true
			}
		}
	}

	if currentPath != "" {
		ensurePathAccumulator(out, currentPath)
	}

	return out
}

func ensurePathAccumulator(target map[string]*folderLineagePathAccumulator, path string) *folderLineagePathAccumulator {
	acc, ok := target[path]
	if ok {
		return acc
	}
	acc = &folderLineagePathAccumulator{
		path:      path,
		stepTypes: map[string]struct{}{},
	}
	target[path] = acc
	return acc
}

func buildSnapshotTransitions(snapshots []*repository.Snapshot) []folderLineagePathTransition {
	ordered := make([]*repository.Snapshot, 0, len(snapshots))
	for _, item := range snapshots {
		if item == nil {
			continue
		}
		ordered = append(ordered, item)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].CreatedAt.Equal(ordered[j].CreatedAt) {
			return strings.TrimSpace(ordered[i].ID) < strings.TrimSpace(ordered[j].ID)
		}
		return ordered[i].CreatedAt.Before(ordered[j].CreatedAt)
	})

	out := make([]folderLineagePathTransition, 0, len(ordered))
	seen := map[string]struct{}{}
	for _, snapshot := range ordered {
		states := parseSnapshotStates(snapshot.After)
		for _, state := range states {
			from := normalizeLineagePath(state.OriginalPath)
			to := normalizeLineagePath(state.CurrentPath)
			if from == "" || to == "" || from == to {
				continue
			}
			edgeType, eventType := classifyPathTransition(from, to)
			if strings.EqualFold(strings.TrimSpace(snapshot.Status), "reverted") {
				eventType = folderLineageEventTypeRollback
			}
			key := strings.Join([]string{
				from,
				to,
				edgeType,
				eventType,
				snapshot.CreatedAt.UTC().Format(time.RFC3339Nano),
			}, "|")
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, folderLineagePathTransition{
				from:       from,
				to:         to,
				edgeType:   edgeType,
				eventType:  eventType,
				occurredAt: snapshot.CreatedAt.UTC(),
				jobID:      strings.TrimSpace(snapshot.JobID),
			})
		}
	}

	return out
}

func buildReviewTransitions(review *FolderLineageReview, existing []folderLineagePathTransition) []folderLineagePathTransition {
	if review == nil {
		return nil
	}

	existingPairs := map[string]struct{}{}
	for _, item := range existing {
		existingPairs[item.from+"|"+item.to] = struct{}{}
	}

	seen := map[string]struct{}{}
	out := make([]folderLineagePathTransition, 0, 4)
	add := func(from, to, stepType string) {
		if from == "" || to == "" || from == to {
			return
		}
		pair := from + "|" + to
		if _, ok := existingPairs[pair]; ok {
			return
		}
		if _, ok := seen[pair]; ok {
			return
		}
		seen[pair] = struct{}{}
		edgeType, eventType := classifyPathTransition(from, to)
		out = append(out, folderLineagePathTransition{
			from:          from,
			to:            to,
			edgeType:      edgeType,
			eventType:     eventType,
			stepType:      strings.TrimSpace(stepType),
			occurredAt:    review.UpdatedAt.UTC(),
			workflowRunID: strings.TrimSpace(review.WorkflowRunID),
			jobID:         strings.TrimSpace(review.JobID),
		})
	}

	for _, step := range review.ExecutedSteps {
		stepType := strings.TrimSpace(step.NodeType)
		if stepType != "rename-node" && stepType != "move-node" {
			continue
		}
		from := normalizeLineagePath(step.SourcePath)
		to := normalizeLineagePath(resolveStepTargetPath(step.SourcePath, step.TargetPath))
		add(from, to, stepType)
	}
	if len(out) > 0 {
		return out
	}

	beforePath := normalizeLineagePath(lineageAnyString(review.Before["path"]))
	afterPath := normalizeLineagePath(lineageAnyString(review.After["path"]))
	add(beforePath, afterPath, "")

	return out
}

func buildObservationFallbackTransitions(pathAccByPath map[string]*folderLineagePathAccumulator) []folderLineagePathTransition {
	type orderedPath struct {
		path      string
		firstSeen time.Time
		hasFirst  bool
	}
	ordered := make([]orderedPath, 0, len(pathAccByPath))
	for path, acc := range pathAccByPath {
		if strings.TrimSpace(path) == "" {
			continue
		}
		ordered = append(ordered, orderedPath{
			path:      path,
			firstSeen: acc.firstSeenAt,
			hasFirst:  acc.hasFirstSeen,
		})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].hasFirst != ordered[j].hasFirst {
			return ordered[i].hasFirst
		}
		if ordered[i].firstSeen.Equal(ordered[j].firstSeen) {
			return ordered[i].path < ordered[j].path
		}
		return ordered[i].firstSeen.Before(ordered[j].firstSeen)
	})

	out := make([]folderLineagePathTransition, 0, len(ordered))
	for idx := 1; idx < len(ordered); idx++ {
		from := ordered[idx-1].path
		to := ordered[idx].path
		if from == to {
			continue
		}
		edgeType, eventType := classifyPathTransition(from, to)
		occurredAt := ordered[idx].firstSeen
		if occurredAt.IsZero() {
			occurredAt = ordered[idx-1].firstSeen
		}
		out = append(out, folderLineagePathTransition{
			from:       from,
			to:         to,
			edgeType:   edgeType,
			eventType:  eventType,
			occurredAt: occurredAt,
		})
	}

	return out
}

func associatePathMetadata(pathAccByPath map[string]*folderLineagePathAccumulator, transitions []folderLineagePathTransition) {
	for _, transition := range transitions {
		if strings.TrimSpace(transition.from) == "" || strings.TrimSpace(transition.to) == "" {
			continue
		}
		fromAcc := ensurePathAccumulator(pathAccByPath, transition.from)
		toAcc := ensurePathAccumulator(pathAccByPath, transition.to)
		if transition.stepType != "" {
			fromAcc.stepTypes[transition.stepType] = struct{}{}
			toAcc.stepTypes[transition.stepType] = struct{}{}
		}
		if fromAcc.workflowRunID == "" {
			fromAcc.workflowRunID = transition.workflowRunID
		}
		if toAcc.workflowRunID == "" {
			toAcc.workflowRunID = transition.workflowRunID
		}
		if fromAcc.jobID == "" {
			fromAcc.jobID = transition.jobID
		}
		if toAcc.jobID == "" {
			toAcc.jobID = transition.jobID
		}
	}
}

func resolveOriginalPath(
	pathAccByPath map[string]*folderLineagePathAccumulator,
	observations []*repository.FolderPathObservation,
	transitions []folderLineagePathTransition,
	currentPath string,
) string {
	first := firstNonNilObservation(observations)
	if first != nil {
		normalized := normalizeLineagePath(first.Path)
		if normalized != "" {
			return normalized
		}
	}

	if len(transitions) > 0 {
		candidate := normalizeLineagePath(transitions[0].from)
		if candidate != "" {
			return candidate
		}
	}

	type orderedPath struct {
		path      string
		firstSeen time.Time
		hasFirst  bool
	}
	ordered := make([]orderedPath, 0, len(pathAccByPath))
	for path, acc := range pathAccByPath {
		ordered = append(ordered, orderedPath{
			path:      path,
			firstSeen: acc.firstSeenAt,
			hasFirst:  acc.hasFirstSeen,
		})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].hasFirst != ordered[j].hasFirst {
			return ordered[i].hasFirst
		}
		if ordered[i].firstSeen.Equal(ordered[j].firstSeen) {
			return ordered[i].path < ordered[j].path
		}
		return ordered[i].firstSeen.Before(ordered[j].firstSeen)
	})
	if len(ordered) > 0 && ordered[0].path != "" {
		return ordered[0].path
	}

	return normalizeLineagePath(currentPath)
}

func buildFolderLineageNodes(
	pathAccByPath map[string]*folderLineagePathAccumulator,
	originalPath string,
	currentPath string,
	artifacts []folderLineageArtifact,
) ([]FolderLineageNode, map[string]string, map[string]string) {
	nodes := make([]FolderLineageNode, 0, len(pathAccByPath)+len(artifacts))
	pathNodeIDByPath := map[string]string{}
	artifactNodeIDByPath := map[string]string{}

	paths := make([]string, 0, len(pathAccByPath))
	for path := range pathAccByPath {
		if strings.TrimSpace(path) == "" {
			continue
		}
		paths = append(paths, path)
	}
	sort.Slice(paths, func(i, j int) bool {
		left := pathAccByPath[paths[i]]
		right := pathAccByPath[paths[j]]
		if left != nil && right != nil {
			if left.hasFirstSeen != right.hasFirstSeen {
				return left.hasFirstSeen
			}
			if left.hasFirstSeen && right.hasFirstSeen && !left.firstSeenAt.Equal(right.firstSeenAt) {
				return left.firstSeenAt.Before(right.firstSeenAt)
			}
		}
		return paths[i] < paths[j]
	})

	for idx, path := range paths {
		acc := pathAccByPath[path]
		nodeType := folderLineageNodeTypeHistoricalPath
		if path == normalizeLineagePath(currentPath) {
			nodeType = folderLineageNodeTypeCurrentPath
		} else if path == normalizeLineagePath(originalPath) {
			nodeType = folderLineageNodeTypeOrigin
		}
		nodeID := fmt.Sprintf("path-%d", idx+1)
		pathNodeIDByPath[path] = nodeID

		var firstSeenAt *time.Time
		if acc != nil && acc.hasFirstSeen {
			value := acc.firstSeenAt
			firstSeenAt = &value
		}
		var lastSeenAt *time.Time
		if acc != nil && acc.hasLastSeen {
			value := acc.lastSeenAt
			lastSeenAt = &value
		}

		nodes = append(nodes, FolderLineageNode{
			ID:            nodeID,
			Type:          nodeType,
			Label:         filepath.Base(path),
			Path:          path,
			FirstSeenAt:   firstSeenAt,
			LastSeenAt:    lastSeenAt,
			StepTypes:     sortedKeys(acc.stepTypes),
			WorkflowRunID: strings.TrimSpace(acc.workflowRunID),
			JobID:         strings.TrimSpace(acc.jobID),
		})
	}

	for idx, artifact := range artifacts {
		if strings.TrimSpace(artifact.path) == "" {
			continue
		}
		nodeID := fmt.Sprintf("artifact-%d", idx+1)
		artifactNodeIDByPath[artifact.path] = nodeID
		stepTypes := make([]string, 0, 1)
		if strings.TrimSpace(artifact.stepType) != "" {
			stepTypes = append(stepTypes, strings.TrimSpace(artifact.stepType))
		}
		nodes = append(nodes, FolderLineageNode{
			ID:            nodeID,
			Type:          folderLineageNodeTypeArtifact,
			Label:         filepath.Base(artifact.path),
			Path:          artifact.path,
			StepTypes:     stepTypes,
			WorkflowRunID: strings.TrimSpace(artifact.workflowRunID),
			JobID:         strings.TrimSpace(artifact.jobID),
		})
	}

	return nodes, pathNodeIDByPath, artifactNodeIDByPath
}

func buildFolderLineageEdges(
	transitions []folderLineagePathTransition,
	artifacts []folderLineageArtifact,
	pathNodeIDByPath map[string]string,
	artifactNodeIDByPath map[string]string,
	currentPath string,
) []FolderLineageEdge {
	edges := make([]FolderLineageEdge, 0, len(transitions)+len(artifacts))
	seen := map[string]struct{}{}

	for idx, transition := range transitions {
		sourceID := strings.TrimSpace(pathNodeIDByPath[transition.from])
		targetID := strings.TrimSpace(pathNodeIDByPath[transition.to])
		if sourceID == "" || targetID == "" {
			continue
		}
		edgeType := strings.TrimSpace(transition.edgeType)
		if edgeType != folderLineageEdgeTypeMovedTo && edgeType != folderLineageEdgeTypeRenamedTo {
			continue
		}
		key := strings.Join([]string{sourceID, targetID, edgeType}, "|")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		var occurredAt *time.Time
		if !transition.occurredAt.IsZero() {
			value := transition.occurredAt
			occurredAt = &value
		}
		edges = append(edges, FolderLineageEdge{
			ID:            fmt.Sprintf("path-edge-%d", idx+1),
			Type:          edgeType,
			Source:        sourceID,
			Target:        targetID,
			OccurredAt:    occurredAt,
			StepType:      strings.TrimSpace(transition.stepType),
			WorkflowRunID: strings.TrimSpace(transition.workflowRunID),
			JobID:         strings.TrimSpace(transition.jobID),
		})
	}

	pathSourceID := pathNodeIDByPath[normalizeLineagePath(currentPath)]
	if strings.TrimSpace(pathSourceID) == "" {
		for _, id := range pathNodeIDByPath {
			pathSourceID = id
			break
		}
	}
	for idx, artifact := range artifacts {
		if strings.TrimSpace(pathSourceID) == "" {
			break
		}
		targetID := strings.TrimSpace(artifactNodeIDByPath[artifact.path])
		if targetID == "" {
			continue
		}
		key := strings.Join([]string{pathSourceID, targetID, folderLineageEdgeTypeProduced}, "|")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		var occurredAt *time.Time
		if !artifact.occurredAt.IsZero() {
			value := artifact.occurredAt
			occurredAt = &value
		}
		edges = append(edges, FolderLineageEdge{
			ID:            fmt.Sprintf("artifact-edge-%d", idx+1),
			Type:          folderLineageEdgeTypeProduced,
			Source:        pathSourceID,
			Target:        targetID,
			OccurredAt:    occurredAt,
			StepType:      strings.TrimSpace(artifact.stepType),
			WorkflowRunID: strings.TrimSpace(artifact.workflowRunID),
			JobID:         strings.TrimSpace(artifact.jobID),
		})
	}

	return edges
}

func parseSnapshotStates(raw json.RawMessage) []folderLineagePathState {
	if len(raw) == 0 {
		return nil
	}

	var arrayPayload []folderLineagePathState
	if err := json.Unmarshal(raw, &arrayPayload); err == nil {
		return arrayPayload
	}

	var singlePayload folderLineagePathState
	if err := json.Unmarshal(raw, &singlePayload); err == nil {
		return []folderLineagePathState{singlePayload}
	}

	return nil
}
