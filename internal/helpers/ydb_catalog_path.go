package helpers

import "strings"

// RelativizeYDBCatalogPath converts a YDB catalog path returned by the control plane (Describe,
// scheme metadata) into the path relative to the database root used in Terraform.
//
// YDB commonly returns absolute paths under the database root, e.g. database "/local" and
// object "/local/my/folder/secret". Resources are addressed by connection_string plus a path
// relative to that database ("my/folder/secret"). When objectPath is under databaseRoot,
// the database prefix is removed; otherwise objectPath is returned unchanged.
func RelativizeYDBCatalogPath(databaseRoot, objectPath string) string {
	if objectPath == "" {
		return ""
	}
	root := strings.TrimSuffix(databaseRoot, "/")
	prefix := root + "/"
	if strings.HasPrefix(objectPath, prefix) {
		return objectPath[len(prefix):]
	}
	return objectPath
}

// JoinYDBCatalogPath is the inverse of RelativizeYDBCatalogPath for paths under databaseRoot:
// it returns databaseRoot+"/"+path when path is not already an absolute path under that root.
// If path is empty, databaseRoot (trimmed) is returned.
func JoinYDBCatalogPath(databaseRoot, path string) string {
	root := strings.TrimSuffix(databaseRoot, "/")
	if path == "" {
		return root
	}
	prefix := root + "/"
	if strings.HasPrefix(path, prefix) || path == root {
		return path
	}
	rel := strings.Trim(path, "/")
	if rel == "" {
		return root
	}
	return prefix + rel
}
