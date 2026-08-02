package engine

import (
	"bytes"
	"fmt"
	"html/template"
)

type TemplateEngine struct{}

func NewTemplateEngine() *TemplateEngine {
	return &TemplateEngine{}
}

func (te *TemplateEngine) Render(tmplStr string, payload map[string]interface{}) (string, error) {
	t, err := template.New("tmpl").Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, payload); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}
