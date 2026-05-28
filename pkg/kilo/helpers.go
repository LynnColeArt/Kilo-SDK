package kilo

import "strings"

func normalizeStringMap(values map[string]string) map[string]string {
	normalized := map[string]string{}
	for key, value := range values {
		cleanKey := strings.TrimSpace(key)
		if cleanKey == "" {
			continue
		}
		normalized[cleanKey] = strings.TrimSpace(value)
	}
	return normalized
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
