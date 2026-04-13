package service

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const renameNodeExecutorType = "rename-node"

type renameNodeExecutor struct{}

type renameConditionalRule struct {
	Condition string
	Template  string
}

func newRenameNodeExecutor() *renameNodeExecutor {
	return &renameNodeExecutor{}
}

func NewRenameNodeExecutor() WorkflowNodeExecutor {
	return newRenameNodeExecutor()
}

func (e *renameNodeExecutor) Type() string {
	return renameNodeExecutorType
}

func (e *renameNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "重命名节点",
		Description: "通过模板、正则或条件规则重命名处理项的目标文件夹名",
		Inputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, Description: "待重命名的处理项列表", Required: true, SkipOnEmpty: true, AcceptDefault: true},
		},
		Outputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, RequiredOutput: true, Description: "已重命名的处理项列表"},
		},
	}
}

func (e *renameNodeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	items, ok := categoryRouterExtractItems(input.Inputs)
	if !ok {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: items input is required", e.Type())
	}

	strategy := strings.ToLower(strings.TrimSpace(stringConfig(input.Node.Config, "strategy")))
	if strategy == "" {
		strategy = "template"
	}

	templateText := stringConfig(input.Node.Config, "template")
	regexPattern := stringConfig(input.Node.Config, "regex")
	if regexPattern == "" {
		regexPattern = stringConfig(input.Node.Config, "pattern")
	}
	skipIfSame := folderSplitterBoolConfig(input.Node.Config, "skip_if_same", false)
	conditionalRules := renameNodeParseConditionalRules(input.Node.Config)

	result := make([]ProcessingItem, 0, len(items))
	stepResults := make([]ProcessingStepResult, 0, len(items))
	for index, item := range items {
		current := renameNodeCurrentName(item)
		variables := renameNodeBuildVariables(item, index+1, nil)

		candidate, err := renameNodeApplyStrategy(strategy, current, item, templateText, regexPattern, variables, conditionalRules)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: %w", e.Type(), err)
		}
		if strings.TrimSpace(candidate) == "" {
			candidate = current
		}

		status := "renamed"
		if skipIfSame && candidate == current {
			status = "skipped"
		}
		if !skipIfSame || candidate != current {
			item.TargetName = candidate
		}
		result = append(result, item)
		stepResults = append(stepResults, ProcessingStepResult{
			FolderID:   strings.TrimSpace(item.FolderID),
			SourcePath: strings.TrimSpace(item.SourcePath),
			TargetPath: strings.TrimSpace(resolveProcessingStepTargetPath(item.SourcePath, candidate)),
			NodeType:   input.Node.Type,
			NodeLabel:  strings.TrimSpace(input.Node.Label),
			Status:     status,
		})
	}

	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{
			"items":        {Type: PortTypeProcessingItemList, Value: result},
			"step_results": {Type: PortTypeProcessingStepResultList, Value: stepResults},
		},
		Status: ExecutionSuccess,
	}, nil
}

func (e *renameNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *renameNodeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func renameNodeApplyStrategy(
	strategy string,
	current string,
	item ProcessingItem,
	templateText string,
	regexPattern string,
	variables map[string]string,
	conditionalRules []renameConditionalRule,
) (string, error) {
	switch strategy {
	case "simple":
		return current, nil
	case "template":
		if strings.TrimSpace(templateText) == "" {
			return current, nil
		}
		return renameNodeRenderTemplate(templateText, variables), nil
	case "regex_extract":
		return renameNodeRegexExtract(current, templateText, regexPattern, variables)
	case "conditional":
		return renameNodeApplyConditional(current, item.Category, variables, conditionalRules)
	default:
		return current, nil
	}
}

func renameNodeRegexExtract(current string, templateText string, regexPattern string, variables map[string]string) (string, error) {
	if strings.TrimSpace(regexPattern) == "" {
		return current, nil
	}

	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return "", fmt.Errorf("regex_extract compile pattern %q: %w", regexPattern, err)
	}

	matches := re.FindStringSubmatch(current)
	if len(matches) == 0 {
		return current, nil
	}

	captured := renameNodeCollectRegexGroups(re, matches)
	for key, value := range captured {
		variables[key] = value
	}

	if strings.TrimSpace(templateText) != "" {
		return renameNodeRenderTemplate(templateText, variables), nil
	}

	if title, ok := captured["title"]; ok && strings.TrimSpace(title) != "" {
		return title, nil
	}

	for key, value := range captured {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		return value, nil
	}

	if len(matches) > 1 && strings.TrimSpace(matches[1]) != "" {
		return matches[1], nil
	}

	return current, nil
}

func renameNodeApplyConditional(current string, category string, variables map[string]string, rules []renameConditionalRule) (string, error) {
	if len(rules) == 0 {
		return current, nil
	}

	defaultTemplate := ""
	for _, rule := range rules {
		condition := strings.TrimSpace(rule.Condition)
		if strings.EqualFold(condition, "DEFAULT") {
			defaultTemplate = rule.Template
			continue
		}

		matched, err := renameNodeEvaluateCondition(condition, current, category)
		if err != nil {
			return "", err
		}
		if matched {
			return renameNodeRenderTemplate(rule.Template, variables), nil
		}
	}

	if defaultTemplate != "" {
		return renameNodeRenderTemplate(defaultTemplate, variables), nil
	}

	return current, nil
}

func renameNodeEvaluateCondition(condition string, name string, category string) (bool, error) {
	trimmed := strings.TrimSpace(condition)
	if trimmed == "" {
		return false, nil
	}

	containsRE := regexp.MustCompile(`(?i)^name\s+CONTAINS\s+"([^"]+)"$`)
	matchesRE := regexp.MustCompile(`(?i)^name\s+MATCHES\s+"([^"]+)"$`)
	categoryRE := regexp.MustCompile(`(?i)^category\s*==\s*"([^"]+)"$`)

	if parts := containsRE.FindStringSubmatch(trimmed); len(parts) == 2 {
		return strings.Contains(strings.ToLower(name), strings.ToLower(parts[1])), nil
	}

	if parts := matchesRE.FindStringSubmatch(trimmed); len(parts) == 2 {
		re, err := regexp.Compile(parts[1])
		if err != nil {
			return false, fmt.Errorf("conditional invalid regex %q: %w", parts[1], err)
		}
		return re.MatchString(name), nil
	}

	if parts := categoryRE.FindStringSubmatch(trimmed); len(parts) == 2 {
		return strings.EqualFold(strings.TrimSpace(category), strings.TrimSpace(parts[1])), nil
	}

	return false, nil
}

func renameNodeParseConditionalRules(config map[string]any) []renameConditionalRule {
	if config == nil {
		return nil
	}

	raw, ok := config["rules"]
	if !ok || raw == nil {
		return nil
	}

	list, ok := raw.([]any)
	if !ok {
		return nil
	}

	out := make([]renameConditionalRule, 0, len(list))
	for _, item := range list {
		ruleMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		condition := strings.TrimSpace(anyString(ruleMap["condition"]))
		if condition == "" {
			condition = strings.TrimSpace(anyString(ruleMap["if"]))
		}
		templateText := strings.TrimSpace(anyString(ruleMap["template"]))
		if templateText == "" {
			continue
		}

		out = append(out, renameConditionalRule{Condition: condition, Template: templateText})
	}

	return out
}

func renameNodeBuildVariables(item ProcessingItem, index int, extra map[string]string) map[string]string {
	name := renameNodeCurrentName(item)
	year := renameNodeExtractYear(name)
	parent := strings.TrimSpace(filepath.Base(strings.TrimSpace(item.ParentPath)))
	if parent == "." || parent == string(filepath.Separator) {
		parent = ""
	}

	vars := map[string]string{
		"name":     name,
		"title":    renameNodeExtractTitle(name, year),
		"category": item.Category,
		"year":     year,
		"index":    strconv.Itoa(index),
		"parent":   parent,
	}

	for key, value := range extra {
		vars[key] = value
	}

	return vars
}

func renameNodeCurrentName(item ProcessingItem) string {
	if strings.TrimSpace(item.TargetName) != "" {
		return strings.TrimSpace(item.TargetName)
	}

	return strings.TrimSpace(item.FolderName)
}

func renameNodeRenderTemplate(templateText string, variables map[string]string) string {
	result := templateText
	for key, value := range variables {
		result = strings.ReplaceAll(result, "{"+key+"}", value)
	}

	return strings.TrimSpace(result)
}

func renameNodeExtractYear(name string) string {
	yearRE := regexp.MustCompile(`(19|20)\d{2}`)
	match := yearRE.FindString(name)
	return strings.TrimSpace(match)
}

func renameNodeExtractTitle(name string, year string) string {
	title := strings.TrimSpace(name)
	if year != "" {
		title = strings.ReplaceAll(title, "("+year+")", "")
		title = strings.ReplaceAll(title, "["+year+"]", "")
		title = strings.ReplaceAll(title, year, "")
	}
	title = strings.TrimSpace(title)
	title = strings.Trim(title, "-_()[] ")
	if title == "" {
		return strings.TrimSpace(name)
	}

	return title
}

func renameNodeCollectRegexGroups(re *regexp.Regexp, matches []string) map[string]string {
	out := map[string]string{}
	names := re.SubexpNames()
	for i := 1; i < len(matches); i++ {
		if i >= len(names) {
			continue
		}
		name := strings.TrimSpace(names[i])
		if name == "" {
			continue
		}
		out[name] = matches[i]
	}

	return out
}
