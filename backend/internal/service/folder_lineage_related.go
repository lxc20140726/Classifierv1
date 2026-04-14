package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/liqiye/classifier/internal/repository"
)

func (s *FolderLineageService) resolveRelatedFolders(ctx context.Context, folder *repository.Folder) ([]*repository.Folder, error) {
	if folder == nil {
		return []*repository.Folder{}, nil
	}

	related := []*repository.Folder{folder}
	if !shouldAggregateFolderLineage(folder) {
		return related, nil
	}

	seen := map[string]*repository.Folder{
		strings.TrimSpace(folder.ID): folder,
	}
	appendUnique := func(item *repository.Folder) {
		if item == nil {
			return
		}
		itemID := strings.TrimSpace(item.ID)
		if itemID == "" {
			return
		}
		if _, ok := seen[itemID]; ok {
			return
		}
		seen[itemID] = item
		related = append(related, item)
	}

	descendants, err := s.folders.ListByPathPrefix(ctx, folder.Path)
	if err != nil {
		return nil, fmt.Errorf("folderLineage.resolveRelatedFolders list descendants for folder %q: %w", strings.TrimSpace(folder.ID), err)
	}
	for _, item := range descendants {
		appendUnique(item)
	}

	relativePath := strings.TrimSpace(folder.RelativePath)
	if relativePath == "" {
		return sortRelatedFolders(folder.ID, related), nil
	}

	candidates, err := s.folders.ListByRelativePath(ctx, relativePath)
	if err != nil {
		return nil, fmt.Errorf("folderLineage.resolveRelatedFolders list relative path candidates for folder %q: %w", strings.TrimSpace(folder.ID), err)
	}
	for _, item := range candidates {
		if item == nil || !strings.EqualFold(strings.TrimSpace(item.CategorySource), "workflow") {
			continue
		}
		matched, matchErr := s.isRelatedWorkflowFolder(ctx, folder, item)
		if matchErr != nil {
			return nil, matchErr
		}
		if matched {
			appendUnique(item)
		}
	}

	return sortRelatedFolders(folder.ID, related), nil
}

func shouldAggregateFolderLineage(folder *repository.Folder) bool {
	if folder == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(folder.CategorySource), "workflow") {
		return false
	}
	relativePath := strings.TrimSpace(folder.RelativePath)
	if relativePath == "" {
		return false
	}
	return !strings.Contains(relativePath, "/") && !strings.Contains(relativePath, `\`)
}

func sortRelatedFolders(baseFolderID string, folders []*repository.Folder) []*repository.Folder {
	ordered := make([]*repository.Folder, 0, len(folders))
	for _, folder := range folders {
		if folder == nil {
			continue
		}
		ordered = append(ordered, folder)
	}

	sort.Slice(ordered, func(i, j int) bool {
		leftID := strings.TrimSpace(ordered[i].ID)
		rightID := strings.TrimSpace(ordered[j].ID)
		if leftID == strings.TrimSpace(baseFolderID) {
			return true
		}
		if rightID == strings.TrimSpace(baseFolderID) {
			return false
		}

		leftPath := normalizeLineagePath(ordered[i].Path)
		rightPath := normalizeLineagePath(ordered[j].Path)
		if len(leftPath) != len(rightPath) {
			return len(leftPath) < len(rightPath)
		}
		if leftPath != rightPath {
			return leftPath < rightPath
		}
		return leftID < rightID
	})

	return ordered
}

func (s *FolderLineageService) isRelatedWorkflowFolder(
	ctx context.Context,
	rootFolder *repository.Folder,
	candidate *repository.Folder,
) (bool, error) {
	if rootFolder == nil || candidate == nil {
		return false, nil
	}
	if strings.TrimSpace(rootFolder.ID) == strings.TrimSpace(candidate.ID) {
		return true, nil
	}

	observations, err := s.folders.ListPathObservationsByFolderID(ctx, candidate.ID)
	if err != nil {
		return false, fmt.Errorf("folderLineage.isRelatedWorkflowFolder list observations for folder %q: %w", strings.TrimSpace(candidate.ID), err)
	}

	return folderObservationMatchesRootPath(rootFolder.Path, observations), nil
}

func folderObservationMatchesRootPath(rootPath string, observations []*repository.FolderPathObservation) bool {
	normalizedRootPath := normalizeLineagePath(rootPath)
	if normalizedRootPath == "" {
		return false
	}

	for _, observation := range observations {
		if observation == nil {
			continue
		}
		observedPath := normalizeLineagePath(observation.Path)
		if observedPath == normalizedRootPath || strings.HasPrefix(observedPath, normalizedRootPath+"/") {
			return true
		}
	}

	return false
}

func (s *FolderLineageService) collectObservationsByFolderID(
	ctx context.Context,
	folders []*repository.Folder,
) (map[string][]*repository.FolderPathObservation, error) {
	observationsByFolderID := make(map[string][]*repository.FolderPathObservation, len(folders))
	for _, folder := range folders {
		if folder == nil {
			continue
		}
		folderID := strings.TrimSpace(folder.ID)
		if folderID == "" {
			continue
		}
		observations, err := s.folders.ListPathObservationsByFolderID(ctx, folderID)
		if err != nil {
			return nil, fmt.Errorf("folderLineage.collectObservationsByFolderID list observations for folder %q: %w", folderID, err)
		}
		observationsByFolderID[folderID] = observations
	}

	return observationsByFolderID, nil
}

func flattenFolderPathObservations(
	observationsByFolderID map[string][]*repository.FolderPathObservation,
) []*repository.FolderPathObservation {
	out := make([]*repository.FolderPathObservation, 0)
	for _, items := range observationsByFolderID {
		out = append(out, items...)
	}
	return out
}

func (s *FolderLineageService) collectSnapshotsForFolders(
	ctx context.Context,
	folders []*repository.Folder,
) ([]*repository.Snapshot, error) {
	if s.snapshots == nil {
		return []*repository.Snapshot{}, nil
	}

	snapshots := make([]*repository.Snapshot, 0)
	for _, folder := range folders {
		if folder == nil {
			continue
		}
		items, err := s.snapshots.ListByFolderID(ctx, folder.ID)
		if err != nil {
			return nil, fmt.Errorf("folderLineage.collectSnapshotsForFolders list snapshots for folder %q: %w", strings.TrimSpace(folder.ID), err)
		}
		snapshots = append(snapshots, items...)
	}
	return snapshots, nil
}

func (s *FolderLineageService) collectLatestReviewsByFolderID(
	ctx context.Context,
	folders []*repository.Folder,
) (map[string]*repository.ProcessingReviewItem, error) {
	reviewsByFolderID := make(map[string]*repository.ProcessingReviewItem, len(folders))
	if s.reviews == nil {
		return reviewsByFolderID, nil
	}

	for _, folder := range folders {
		if folder == nil {
			continue
		}
		item, err := s.reviews.GetLatestByFolderID(ctx, folder.ID)
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("folderLineage.collectLatestReviewsByFolderID get latest review for folder %q: %w", strings.TrimSpace(folder.ID), err)
		}
		reviewsByFolderID[strings.TrimSpace(folder.ID)] = item
	}

	return reviewsByFolderID, nil
}

func orderedFolderReviews(reviewsByFolderID map[string]*repository.ProcessingReviewItem) []*repository.ProcessingReviewItem {
	reviews := make([]*repository.ProcessingReviewItem, 0, len(reviewsByFolderID))
	for _, item := range reviewsByFolderID {
		if item == nil {
			continue
		}
		reviews = append(reviews, item)
	}

	sort.Slice(reviews, func(i, j int) bool {
		left := folderReviewComparisonTime(reviews[i])
		right := folderReviewComparisonTime(reviews[j])
		if left.Equal(right) {
			return strings.TrimSpace(reviews[i].ID) < strings.TrimSpace(reviews[j].ID)
		}
		return left.Before(right)
	})

	return reviews
}

func selectLatestFolderReview(reviewsByFolderID map[string]*repository.ProcessingReviewItem) *repository.ProcessingReviewItem {
	var latest *repository.ProcessingReviewItem
	for _, item := range reviewsByFolderID {
		if item == nil {
			continue
		}
		if latest == nil || folderReviewComparisonTime(item).After(folderReviewComparisonTime(latest)) {
			latest = item
		}
	}
	return latest
}

func folderReviewComparisonTime(item *repository.ProcessingReviewItem) time.Time {
	if item == nil {
		return time.Time{}
	}
	if item.ReviewedAt != nil && !item.ReviewedAt.IsZero() {
		return item.ReviewedAt.UTC()
	}
	if !item.UpdatedAt.IsZero() {
		return item.UpdatedAt.UTC()
	}
	return item.CreatedAt.UTC()
}

func relatedFolderIDs(folders []*repository.Folder) []string {
	ids := make([]string, 0, len(folders))
	seen := make(map[string]struct{}, len(folders))
	for _, folder := range folders {
		if folder == nil {
			continue
		}
		folderID := strings.TrimSpace(folder.ID)
		if folderID == "" {
			continue
		}
		if _, ok := seen[folderID]; ok {
			continue
		}
		seen[folderID] = struct{}{}
		ids = append(ids, folderID)
	}
	return ids
}
