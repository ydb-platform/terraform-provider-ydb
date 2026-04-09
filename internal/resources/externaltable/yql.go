package externaltable

func PrepareCreateQuery(fullPath string, r *Resource) string {
	buf := make([]byte, 0, 512)
	buf = append(buf, "CREATE EXTERNAL TABLE `"...)
	buf = append(buf, fullPath...)
	buf = append(buf, "` ("...)

	for i, col := range r.Columns {
		buf = append(buf, " `"...)
		buf = append(buf, col.Name...)
		buf = append(buf, "` "...)
		buf = append(buf, col.Type...)
		if col.NotNull {
			buf = append(buf, " NOT NULL"...)
		}
		if i < len(r.Columns)-1 {
			buf = append(buf, ',')
		}
	}

	buf = append(buf, " ) WITH ("...)
	buf = append(buf, " DATA_SOURCE = \""...)
	buf = append(buf, r.DataSourcePath...)
	buf = append(buf, "\","...)
	buf = append(buf, " LOCATION = \""...)
	buf = append(buf, r.Location...)
	buf = append(buf, '"')

	if r.Format != "" {
		buf = append(buf, ", FORMAT = \""...)
		buf = append(buf, r.Format...)
		buf = append(buf, '"')
	}
	if r.Compression != "" {
		buf = append(buf, ", COMPRESSION = \""...)
		buf = append(buf, r.Compression...)
		buf = append(buf, '"')
	}

	buf = append(buf, " )"...)
	return string(buf)
}

func PrepareDropQuery(fullPath string) string {
	buf := make([]byte, 0, 128)
	buf = append(buf, "DROP EXTERNAL TABLE `"...)
	buf = append(buf, fullPath...)
	buf = append(buf, '`')
	return string(buf)
}
