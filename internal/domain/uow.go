package domain

import "context"

// UnitOfWork abstracts a database transaction so use-cases can coordinate
// multiple repository operations atomically without importing GORM or any
// other ORM. Implementations live in internal/adapter/persistence.
//
// Usage in a use-case:
//
//	err := uow.Execute(ctx, func(ctx context.Context) error {
//	    if err := paymentRepo.Save(ctx, uow, payment); err != nil {
//	        return err
//	    }
//	    return outboxRepo.Enqueue(ctx, uow, msg)
//	})
type UnitOfWork interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
}
