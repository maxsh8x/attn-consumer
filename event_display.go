package main

import (
	"database/sql"

	"github.com/kshvakov/clickhouse"
	common "gitlab.com/maksimsharnin/attn-common"
)

func execDisplayEvent(stmt *sql.Stmt, msg *common.RabbitMSG) (sql.Result, error) {
	return stmt.Exec(
		clickhouse.DateTime(msg.Date),
		BoolToUInt8(msg.Mobile),
		msg.Platform,
		msg.OS,
		msg.Browser,
		msg.Version,
		clickhouse.IP(msg.IP),
		msg.OfferID,
		msg.PlacementID,
		msg.WidgetID,
	)
}

func initDisplayEvent() {
	createQueries["display"] = `
		CREATE TABLE IF NOT EXISTS display (
			date DateTime,
			mobile UInt8,
			platform String,
			os String,
			browser String,
			version String,
			ip FixedString(16),
			offer_id UInt32,
			placement_id UInt32,
			widget_id UInt32
		) engine=Memory`

	insertQueries["display"] = `
		INSERT INTO display (
			date,
			mobile,
			platform,
			os,
			browser,
			version,
			ip,
			offer_id,
			placement_id,
			widget_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	execFunctions["display"] = execDisplayEvent
}
