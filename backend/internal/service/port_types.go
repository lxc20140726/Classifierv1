package service

import (
	"encoding/json"
	"fmt"
)

type PortType string

const (
	PortTypeJSON                     PortType = "JSON"
	PortTypePath                     PortType = "PATH"
	PortTypeString                   PortType = "STRING"
	PortTypeBoolean                  PortType = "BOOLEAN"
	PortTypeStringList               PortType = "STRING_LIST"
	PortTypeFolderTreeList           PortType = "FOLDER_TREE_LIST"
	PortTypeClassificationSignalList PortType = "CLASSIFICATION_SIGNAL_LIST"
	PortTypeClassifiedEntryList      PortType = "CLASSIFIED_ENTRY_LIST"
	PortTypeProcessingItemList       PortType = "PROCESSING_ITEM_LIST"
	PortTypeProcessingStepResultList PortType = "PROCESSING_STEP_RESULT_LIST"
)

type TypedValue struct {
	Type  PortType `json:"type"`
	Value any      `json:"-"`
}

type TypedValueJSON struct {
	Type  PortType        `json:"type"`
	Value json.RawMessage `json:"value"`
}

type TypeRegistry struct {
	marshalers   map[PortType]func(any) (json.RawMessage, error)
	unmarshalers map[PortType]func(json.RawMessage) (any, error)
}

func NewTypeRegistry() *TypeRegistry {
	r := &TypeRegistry{
		marshalers:   make(map[PortType]func(any) (json.RawMessage, error)),
		unmarshalers: make(map[PortType]func(json.RawMessage) (any, error)),
	}

	r.Register(PortTypePath,
		func(v any) (json.RawMessage, error) { return json.Marshal(v.(string)) },
		func(raw json.RawMessage) (any, error) {
			var out string
			return out, json.Unmarshal(raw, &out)
		},
	)
	r.Register(PortTypeJSON,
		func(v any) (json.RawMessage, error) { return json.Marshal(v) },
		func(raw json.RawMessage) (any, error) {
			var out any
			return out, json.Unmarshal(raw, &out)
		},
	)
	r.Register(PortTypeString,
		func(v any) (json.RawMessage, error) { return json.Marshal(v.(string)) },
		func(raw json.RawMessage) (any, error) {
			var out string
			return out, json.Unmarshal(raw, &out)
		},
	)
	r.Register(PortTypeBoolean,
		func(v any) (json.RawMessage, error) { return json.Marshal(v.(bool)) },
		func(raw json.RawMessage) (any, error) {
			var out bool
			return out, json.Unmarshal(raw, &out)
		},
	)
	r.Register(PortTypeStringList,
		func(v any) (json.RawMessage, error) { return json.Marshal(v.([]string)) },
		func(raw json.RawMessage) (any, error) {
			var out []string
			return out, json.Unmarshal(raw, &out)
		},
	)
	r.Register(PortTypeFolderTreeList,
		func(v any) (json.RawMessage, error) { return json.Marshal(v.([]FolderTree)) },
		func(raw json.RawMessage) (any, error) {
			var out []FolderTree
			return out, json.Unmarshal(raw, &out)
		},
	)
	r.Register(PortTypeClassificationSignalList,
		func(v any) (json.RawMessage, error) { return json.Marshal(v.([]ClassificationSignal)) },
		func(raw json.RawMessage) (any, error) {
			var out []ClassificationSignal
			return out, json.Unmarshal(raw, &out)
		},
	)
	r.Register(PortTypeClassifiedEntryList,
		func(v any) (json.RawMessage, error) { return json.Marshal(v.([]ClassifiedEntry)) },
		func(raw json.RawMessage) (any, error) {
			var out []ClassifiedEntry
			return out, json.Unmarshal(raw, &out)
		},
	)
	r.Register(PortTypeProcessingItemList,
		func(v any) (json.RawMessage, error) { return json.Marshal(v.([]ProcessingItem)) },
		func(raw json.RawMessage) (any, error) {
			var out []ProcessingItem
			return out, json.Unmarshal(raw, &out)
		},
	)
	r.Register(PortTypeProcessingStepResultList,
		func(v any) (json.RawMessage, error) { return json.Marshal(v.([]ProcessingStepResult)) },
		func(raw json.RawMessage) (any, error) {
			var out []ProcessingStepResult
			return out, json.Unmarshal(raw, &out)
		},
	)

	return r
}

func (r *TypeRegistry) Register(portType PortType, marshal func(any) (json.RawMessage, error), unmarshal func(json.RawMessage) (any, error)) {
	r.marshalers[portType] = marshal
	r.unmarshalers[portType] = unmarshal
}

func (r *TypeRegistry) Marshal(tv TypedValue) (TypedValueJSON, error) {
	marshal, ok := r.marshalers[tv.Type]
	if !ok {
		return TypedValueJSON{}, fmt.Errorf("typeRegistry.Marshal: unsupported port type %q", tv.Type)
	}

	if tv.Value == nil {
		return TypedValueJSON{Type: tv.Type, Value: []byte("null")}, nil
	}

	raw, err := marshal(tv.Value)
	if err != nil {
		return TypedValueJSON{}, fmt.Errorf("typeRegistry.Marshal %q: %w", tv.Type, err)
	}

	return TypedValueJSON{Type: tv.Type, Value: raw}, nil
}

func (r *TypeRegistry) Unmarshal(tvj TypedValueJSON) (TypedValue, error) {
	unmarshal, ok := r.unmarshalers[tvj.Type]
	if !ok {
		return TypedValue{}, fmt.Errorf("typeRegistry.Unmarshal: unsupported port type %q", tvj.Type)
	}

	if len(tvj.Value) == 0 || string(tvj.Value) == "null" {
		return TypedValue{Type: tvj.Type, Value: nil}, nil
	}

	value, err := unmarshal(tvj.Value)
	if err != nil {
		return TypedValue{}, fmt.Errorf("typeRegistry.Unmarshal %q: %w", tvj.Type, err)
	}

	return TypedValue{Type: tvj.Type, Value: value}, nil
}
