package persistence

import "gorm.io/gorm"

func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&OutboxMessageModel{}, &InboxMessageModel{}, &RecordModel{})
}
