package service

import (
	"reflect"
	"testing"
)

func TestTypeRegistryRoundTrip(t *testing.T) {
	t.Parallel()

	registry := NewTypeRegistry()
	tests := []struct {
		name  string
		value TypedValue
	}{
		{name: "path", value: TypedValue{Type: PortTypePath, Value: "/data/source"}},
		{name: "string", value: TypedValue{Type: PortTypeString, Value: "abc"}},
		{name: "boolean", value: TypedValue{Type: PortTypeBoolean, Value: true}},
		{name: "string list", value: TypedValue{Type: PortTypeStringList, Value: []string{"a", "b"}}},
		{name: "folder trees", value: TypedValue{Type: PortTypeFolderTreeList, Value: []FolderTree{{Path: "/data/a", Name: "a", Files: []FileEntry{{Name: "cover.jpg", Ext: ".jpg", SizeBytes: 100}}}}}},
		{name: "classification signals", value: TypedValue{Type: PortTypeClassificationSignalList, Value: []ClassificationSignal{{SourcePath: "/data/a", Category: "manga", Confidence: 1, Reason: "kw"}}}},
		{name: "classified entries", value: TypedValue{Type: PortTypeClassifiedEntryList, Value: []ClassifiedEntry{{Path: "/data/a", Name: "a", Category: "manga"}}}},
		{name: "processing items", value: TypedValue{Type: PortTypeProcessingItemList, Value: []ProcessingItem{{SourcePath: "/data/a", FolderID: "folder-a", Category: "manga"}}}},
		{name: "processing step results", value: TypedValue{Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: "/data/a", TargetPath: "/target/a", NodeType: "move-node", NodeLabel: "移动", Status: "succeeded"}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := registry.Marshal(tt.value)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}

			decoded, err := registry.Unmarshal(encoded)
			if err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}

			if decoded.Type != tt.value.Type {
				t.Fatalf("decoded.Type = %q, want %q", decoded.Type, tt.value.Type)
			}

			if !reflect.DeepEqual(decoded.Value, tt.value.Value) {
				t.Fatalf("decoded.Value = %#v, want %#v", decoded.Value, tt.value.Value)
			}
		})
	}
}

func TestTypeRegistryUnmarshalNull(t *testing.T) {
	t.Parallel()

	registry := NewTypeRegistry()
	decoded, err := registry.Unmarshal(TypedValueJSON{Type: PortTypeString, Value: []byte("null")})
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if decoded.Value != nil {
		t.Fatalf("decoded.Value = %#v, want nil", decoded.Value)
	}
}
