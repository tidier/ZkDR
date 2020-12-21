package zk

import "strings"

// 生成zk路径层级列表
func MakePathToken(path string) []string {
	items := strings.Split(path, "/")
	result := make([]string, len(items))
	parentPath := ""
	size := 0
	for _, v := range items {
		if v == "" {
			continue
		}
		result[size] = parentPath + "/" + v
		parentPath = result[size]
		size++
	}
	return result[:size]
}

// ZkJoinPath 拼接zk路径
func ZkJoinPath(path ...string) string {
	result := ""
	for _, s := range path {
		s = strings.ReplaceAll(s, "//", "/")
		if !strings.HasSuffix(result, "/") && !strings.HasPrefix(s, "/") {
			result += "/"
		}
		if strings.HasSuffix(s, "/") {

		}
		result += s
	}
	if !strings.HasPrefix(result, "/") {
		result = "/" + result
	}
	if strings.HasSuffix(result, "/") {
		return result[:len(result)-1]
	}
	return result
}
