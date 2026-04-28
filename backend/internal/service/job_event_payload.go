package service

import (
	"context"

	"github.com/liqiye/classifier/internal/repository"
)

func attachJobTiming(ctx context.Context, jobs repository.JobRepository, jobID string, payload map[string]any) map[string]any {
	if jobs == nil || jobID == "" {
		return payload
	}

	job, err := jobs.GetByID(ctx, jobID)
	if err != nil || job == nil {
		return payload
	}

	payload["started_at"] = job.StartedAt
	payload["finished_at"] = job.FinishedAt
	payload["updated_at"] = job.UpdatedAt
	return payload
}
