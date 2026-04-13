package service

type FileEntry struct {
	Name      string `json:"name"`
	Ext       string `json:"ext"`
	SizeBytes int64  `json:"size_bytes"`
}

type FolderTree struct {
	Path    string       `json:"path"`
	Name    string       `json:"name"`
	Files   []FileEntry  `json:"files"`
	Subdirs []FolderTree `json:"subdirs"`
}

type ClassificationSignal struct {
	SourcePath string   `json:"source_path"`
	Category   string   `json:"category"`
	Confidence float64  `json:"confidence"`
	Reason     string   `json:"reason"`
	Signals    []string `json:"signals,omitempty"`
	IsEmpty    bool     `json:"is_empty"`
}

type ClassifiedEntry struct {
	FolderID      string            `json:"folder_id"`
	Path          string            `json:"path"`
	Name          string            `json:"name"`
	Category      string            `json:"category"`
	Confidence    float64           `json:"confidence"`
	Reason        string            `json:"reason"`
	Classifier    string            `json:"classifier"`
	HasOtherFiles bool              `json:"has_other_files"`
	Files         []FileEntry       `json:"files"`
	Subtree       []ClassifiedEntry `json:"subtree,omitempty"`
}

type ProcessingItem struct {
	SourcePath         string      `json:"source_path"`
	CurrentPath        string      `json:"current_path"`
	FolderID           string      `json:"folder_id"`
	FolderName         string      `json:"folder_name"`
	TargetName         string      `json:"target_name"`
	Category           string      `json:"category"`
	Files              []FileEntry `json:"files"`
	ParentPath         string      `json:"parent_path"`
	RootPath           string      `json:"root_path"`
	RelativePath       string      `json:"relative_path"`
	SourceKind         string      `json:"source_kind"`
	OriginalSourcePath string      `json:"original_source_path"`
}

type MoveResult struct {
	SourcePath string `json:"source_path"`
	TargetPath string `json:"target_path"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
}

type ProcessingStepResult struct {
	FolderID   string `json:"folder_id,omitempty"`
	SourcePath string `json:"source_path"`
	TargetPath string `json:"target_path,omitempty"`
	NodeType   string `json:"node_type"`
	NodeLabel  string `json:"node_label"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
}

const (
	ProcessingItemSourceKindDirectory = "directory"
	ProcessingItemSourceKindArchive   = "archive"
)
