package main

func dropAndCreatePublication(conn *pgconn.PgConn) error {
	_, err := conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS pglogrepl_demo;")
	if err != nil {
		return err
	}

	_, err = conn.Exec(context.Background(), "CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;")
	if err != nil {
		return err
	}
	log.Println("Create publication pglogrepl_demo")
	return nil
}

func getPluginArguments(outputPlugin string) []string {
	switch outputPlugin {
	case OutputPluginPGOutput:
		return []string{"proto_version '1'", "publication_names 'pglogrepl_demo'"}
	case OutputPluginWal2JSON:
		return []string{"\"pretty-print\" 'true'"}
	default:
		log.Fatalf("unknown output plugin: %s", outputPlugin)
	}
	return nil
}

func createReplicationSlot(conn *pgconn.PgConn, slotName, outputPlugin string) error {
	options := pglogrepl.CreateReplicationSlotOptions{Temporary: true}
	_, err := pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, options)
	return err
}

func startReplication(conn *pgconn.PgConn, slotName, xLogPos string, pluginArguments []string) error {
	options := pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}
	return pglogrepl.StartReplication(context.Background(), conn, slotName, xLogPos, options)
}

func sendStandbyStatusUpdate(conn *pgconn.PgConn, xLogPos string) error {
	statusUpdate := pglogrepl.StandbyStatusUpdate{WALWritePosition: xLogPos}
	return pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, statusUpdate)
}
