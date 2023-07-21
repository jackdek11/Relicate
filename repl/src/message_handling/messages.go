package main


func handleInsertMessage(msg *pglogrepl.InsertMessage, relations map[uint32]*pglogrepl.RelationMessage) {
	rel, ok := relations[msg.RelationID]
	if !ok {
		log.Fatalf("unknown relation ID %d", msg.RelationID)
	}
	values := map[string]interface{}{}
	for idx, col := range msg.Tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(col.Data, rel.Columns[idx].DataType)
			if err != nil {
				log.Fatalln("error decoding column data:", err)
			}
			values[colName] = val
		}
	}
	log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)
}
