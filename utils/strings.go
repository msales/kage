package utils

import "strings"

// SplitMap splits a slice of strings into a map of strings using
// the given separator.
func SplitMap(s []string, sep string) map[string]string {
	if len(s) == 0 || sep == "" {
		return nil
	}
	m := make(map[string]string)
	for _, str := range s {
		parts := strings.SplitN(str, sep, 2)
		v := ""
		if len(parts) > 1 {
			v = parts[1]
		}
		m[parts[0]] = v
	}
	return m
}
