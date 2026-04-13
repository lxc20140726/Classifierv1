package service

import (
	"context"
	"fmt"
	"strings"
)

type keywordRule struct {
	Keyword    string  `json:"keyword"`
	Category   string  `json:"category"`
	Confidence float64 `json:"confidence"`
	Priority   int     `json:"priority"`
	MatchType  string  `json:"match_type"`
}

type nameKeywordClassifierConfig struct {
	Rules []keywordRule `json:"rules"`
}

var defaultKeywordRules = []keywordRule{
	{Keyword: "漫画", Category: "manga", Confidence: 1.0, Priority: 10, MatchType: "folder_name_contains"},
	{Keyword: "comic", Category: "manga", Confidence: 1.0, Priority: 10, MatchType: "folder_name_contains"},
	{Keyword: "manga", Category: "manga", Confidence: 1.0, Priority: 10, MatchType: "folder_name_contains"},
	{Keyword: "写真", Category: "photo", Confidence: 0.9, Priority: 9, MatchType: "folder_name_contains"},
	{Keyword: "相册", Category: "photo", Confidence: 0.9, Priority: 9, MatchType: "folder_name_contains"},
}

type nameKeywordClassifierNodeExecutor struct{}

func newNameKeywordClassifierExecutor() *nameKeywordClassifierNodeExecutor {
	return &nameKeywordClassifierNodeExecutor{}
}

func NewNameKeywordClassifierExecutor() WorkflowNodeExecutor {
	return newNameKeywordClassifierExecutor()
}

func (e *nameKeywordClassifierNodeExecutor) Type() string {
	return "name-keyword-classifier"
}

func (e *nameKeywordClassifierNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "关键词分类器",
		Description: "根据文件夹名称关键词进行分类",
		Inputs: []PortDef{
			{Name: "trees", Type: PortTypeFolderTreeList, Description: "目录树列表", Required: false},
		},
		Outputs: []PortDef{
			{Name: "signal", Type: PortTypeClassificationSignalList, RequiredOutput: true, Description: "分类信号列表"},
			{Name: "pass", Type: PortTypeFolderTreeList, RequiredOutput: true, AllowEmpty: true, Description: "未命中透传目录树"},
		},
	}
}

func (e *nameKeywordClassifierNodeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	rawInputs := typedInputsToAny(input.Inputs)
	rawTrees, ok := firstPresent(rawInputs, "trees")
	if !ok {
		return NodeExecutionOutput{Outputs: map[string]TypedValue{"signal": {Type: PortTypeClassificationSignalList, Value: nil}, "pass": {Type: PortTypeFolderTreeList, Value: nil}}, Status: ExecutionSuccess}, nil
	}

	trees, found, err := parseFolderTreesInput(rawTrees)
	if err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute parse trees: %w", e.Type(), err)
	}
	if !found {
		return NodeExecutionOutput{Outputs: map[string]TypedValue{"signal": {Type: PortTypeClassificationSignalList, Value: nil}, "pass": {Type: PortTypeFolderTreeList, Value: nil}}, Status: ExecutionSuccess}, nil
	}

	rules := parseNameKeywordRules(input.Node.Config)
	rules = sortRulesByPriorityDesc(rules)

	signals := make([]ClassificationSignal, 0, len(trees))
	unresolved := make([]FolderTree, 0, len(trees))

	for _, tree := range trees {
		signal := ClassificationSignal{SourcePath: tree.Path, IsEmpty: true}
		folderNameLower := strings.ToLower(tree.Name)
		for _, rule := range rules {
			if rule.MatchType != "folder_name_contains" {
				continue
			}
			if strings.Contains(folderNameLower, strings.ToLower(rule.Keyword)) {
				signal = ClassificationSignal{
					SourcePath: tree.Path,
					Category:   rule.Category,
					Confidence: rule.Confidence,
					Reason:     fmt.Sprintf("keyword:%s", rule.Keyword),
				}
				break
			}
		}

		signals = append(signals, signal)
		if signal.IsEmpty {
			unresolved = append(unresolved, tree)
		}
	}

	return NodeExecutionOutput{Outputs: map[string]TypedValue{"signal": {Type: PortTypeClassificationSignalList, Value: signals}, "pass": {Type: PortTypeFolderTreeList, Value: unresolved}}, Status: ExecutionSuccess}, nil
}

func (e *nameKeywordClassifierNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *nameKeywordClassifierNodeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func parseNameKeywordRules(config map[string]any) []keywordRule {
	if config == nil {
		return append([]keywordRule(nil), defaultKeywordRules...)
	}

	rawRules, ok := config["rules"]
	if !ok {
		return append([]keywordRule(nil), defaultKeywordRules...)
	}

	parsed := make([]keywordRule, 0)
	switch v := rawRules.(type) {
	case []keywordRule:
		parsed = append(parsed, v...)
	case []any:
		for _, item := range v {
			rule, ok := mapToKeywordRule(item)
			if !ok {
				continue
			}
			parsed = append(parsed, rule)
		}
	}

	if len(parsed) == 0 {
		return append([]keywordRule(nil), defaultKeywordRules...)
	}

	return parsed
}

func mapToKeywordRule(raw any) (keywordRule, bool) {
	item, ok := raw.(map[string]any)
	if !ok {
		return keywordRule{}, false
	}

	keyword, ok := item["keyword"].(string)
	if !ok || strings.TrimSpace(keyword) == "" {
		return keywordRule{}, false
	}

	category, _ := item["category"].(string)
	matchType, _ := item["match_type"].(string)

	confidence := toFloat64(item["confidence"])
	priority := toInt(item["priority"])

	return keywordRule{
		Keyword:    keyword,
		Category:   category,
		Confidence: confidence,
		Priority:   priority,
		MatchType:  matchType,
	}, true
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	default:
		return 0
	}
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case int32:
		return int(n)
	case float64:
		return int(n)
	case float32:
		return int(n)
	default:
		return 0
	}
}

func sortRulesByPriorityDesc(rules []keywordRule) []keywordRule {
	sorted := append([]keywordRule(nil), rules...)
	for i := 1; i < len(sorted); i++ {
		current := sorted[i]
		j := i - 1
		for j >= 0 && sorted[j].Priority < current.Priority {
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = current
	}

	return sorted
}
