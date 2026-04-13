package service

func testInputs(values map[string]any) map[string]*TypedValue {
	out := make(map[string]*TypedValue, len(values))
	for key, value := range values {
		out[key] = &TypedValue{Type: PortTypeJSON, Value: value}
	}

	return out
}
