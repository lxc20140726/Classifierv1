package service

import "fmt"

func parseClassifiedEntryList(raw any) ([]ClassifiedEntry, error) {
	if raw == nil {
		return []ClassifiedEntry{}, nil
	}

	if entries, ok := raw.([]ClassifiedEntry); ok {
		return entries, nil
	}

	rawSlice, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("entries must be an array")
	}

	result := make([]ClassifiedEntry, 0, len(rawSlice))
	for i, item := range rawSlice {
		switch v := item.(type) {
		case ClassifiedEntry:
			result = append(result, v)
		case map[string]any:
			entry, err := classifiedEntryFromMap(v)
			if err != nil {
				return nil, fmt.Errorf("entries[%d]: %w", i, err)
			}
			result = append(result, entry)
		default:
			return nil, fmt.Errorf("entries[%d]: unsupported type %T", i, item)
		}
	}
	return result, nil
}

func classifiedEntryFromMap(m map[string]any) (ClassifiedEntry, error) {
	entry := ClassifiedEntry{}
	if v, ok := m["folder_id"].(string); ok {
		entry.FolderID = v
	}
	if v, ok := m["path"].(string); ok {
		entry.Path = v
	}
	if v, ok := m["name"].(string); ok {
		entry.Name = v
	}
	if v, ok := m["category"].(string); ok {
		entry.Category = v
	}
	if v, ok := m["confidence"].(float64); ok {
		entry.Confidence = v
	}
	if v, ok := m["reason"].(string); ok {
		entry.Reason = v
	}
	if v, ok := m["classifier"].(string); ok {
		entry.Classifier = v
	}
	if v, ok := m["has_other_files"].(bool); ok {
		entry.HasOtherFiles = v
	}
	if subtreeRaw, ok := m["subtree"]; ok {
		subtree, err := parseClassifiedEntryList(subtreeRaw)
		if err != nil {
			return ClassifiedEntry{}, fmt.Errorf("subtree parse failed: %w", err)
		}
		entry.Subtree = subtree
	}
	return entry, nil
}
