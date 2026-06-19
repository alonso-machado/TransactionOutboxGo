package persistence

import (
	"context"

	"gorm.io/gorm"
)

type contextKey string

const txKey contextKey = "tx"

type GORMUnitOfWork struct {
	db *gorm.DB
}

func NewUnitOfWork(db *gorm.DB) *GORMUnitOfWork {
	return &GORMUnitOfWork{db: db}
}

func (u *GORMUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	return u.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(context.WithValue(ctx, txKey, tx))
	})
}

func TxFromContext(ctx context.Context, db *gorm.DB) *gorm.DB {
	if tx, ok := ctx.Value(txKey).(*gorm.DB); ok {
		return tx
	}
	return db.WithContext(ctx)
}
