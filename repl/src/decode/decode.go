package main

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	mi := pgtype.NewMap()
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}