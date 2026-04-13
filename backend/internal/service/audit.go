package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/liqiye/classifier/internal/repository"
)

var errNilAuditLog = errors.New("audit log is nil")

type AuditService struct {
	repo       repository.AuditRepository
	fileWriter io.Writer
}

func NewAuditService(repo repository.AuditRepository) *AuditService {
	return &AuditService{repo: repo}
}

// SetFileWriter attaches a writer (e.g. a rotating lumberjack file) that
// receives every audit entry as a JSON line in addition to the DB write.
func (s *AuditService) SetFileWriter(w io.Writer) {
	s.fileWriter = w
}

func (s *AuditService) Write(ctx context.Context, log *repository.AuditLog) error {
	if log == nil {
		return errNilAuditLog
	}

	if log.ID == "" {
		log.ID = uuid.NewString()
	}

	if log.CreatedAt.IsZero() {
		log.CreatedAt = time.Now().UTC()
	}

	if log.Level == "" {
		log.Level = "info"
	}

	if err := s.repo.Write(ctx, log); err != nil {
		return err
	}

	if s.fileWriter != nil {
		if b, err := json.Marshal(log); err == nil {
			_, _ = fmt.Fprintf(s.fileWriter, "%s\n", b)
		}
	}

	return nil
}

func (s *AuditService) List(ctx context.Context, filter repository.AuditListFilter) ([]*repository.AuditLog, int, error) {
	return s.repo.List(ctx, filter)
}
