CREATE TABLE IF NOT EXISTS processing_review_items (
    id                TEXT PRIMARY KEY,
    workflow_run_id   TEXT NOT NULL,
    job_id            TEXT NOT NULL,
    folder_id         TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',
    before_json       TEXT,
    after_json        TEXT,
    step_results_json TEXT,
    diff_json         TEXT,
    error             TEXT,
    reviewed_at       DATETIME,
    created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_processing_review_items_workflow_run_id ON processing_review_items(workflow_run_id);
CREATE INDEX IF NOT EXISTS idx_processing_review_items_job_id ON processing_review_items(job_id);
CREATE INDEX IF NOT EXISTS idx_processing_review_items_folder_id ON processing_review_items(folder_id);
CREATE INDEX IF NOT EXISTS idx_processing_review_items_status ON processing_review_items(status);
