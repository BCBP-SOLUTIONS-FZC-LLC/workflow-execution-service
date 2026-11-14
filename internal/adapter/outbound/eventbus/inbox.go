package eventbus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/inbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.ProcessedEventRepository = (*Inbox)(nil)

type Inbox struct {
	order  []string
	stores map[string]*inbox.Store
}

func NewInbox(pool *pgcommon.Pool, consumers ...string) (*Inbox, error) {
	stores := make(map[string]*inbox.Store, len(consumers))
	for _, consumer := range consumers {
		store, err := inbox.NewStore(pool, consumer)
		if err != nil {
			return nil, fmt.Errorf("inbox store %q: %w", consumer, err)
		}
		stores[consumer] = store
	}
	return &Inbox{order: consumers, stores: stores}, nil
}

func (i *Inbox) IsProcessed(ctx context.Context, eventID uuid.UUID, consumer string) (bool, error) {
	store, err := i.store(consumer)
	if err != nil {
		return false, err
	}
	processed, err := store.IsProcessed(ctx, eventID)
	if err != nil {
		return false, fmt.Errorf("consumer %q: %w", consumer, err)
	}
	return processed, nil
}

func (i *Inbox) MarkProcessed(ctx context.Context, eventID uuid.UUID, consumer string) error {
	store, err := i.store(consumer)
	if err != nil {
		return err
	}
	if err := store.MarkProcessed(ctx, eventID); err != nil {
		return fmt.Errorf("consumer %q: %w", consumer, err)
	}
	return nil
}

func (i *Inbox) Prune(ctx context.Context, olderThan time.Duration) (int64, error) {
	return i.PruneBatch(ctx, olderThan, 0)
}

// PruneBatch prunes every consumer's rows in statements of at most batch rows (the library default when batch
// is not positive). A consumer that fails does not stop the others; the errors come back joined.
func (i *Inbox) PruneBatch(ctx context.Context, olderThan time.Duration, batch int) (int64, error) {
	var total int64
	var errs []error
	for _, consumer := range i.order {
		deleted, err := i.stores[consumer].Prune(ctx, olderThan, batch)
		total += deleted
		if err != nil {
			errs = append(errs, fmt.Errorf("prune %q: %w", consumer, err))
		}
	}
	return total, errors.Join(errs...)
}

func (i *Inbox) store(consumer string) (*inbox.Store, error) {
	store, ok := i.stores[consumer]
	if !ok {
		return nil, fmt.Errorf("inbox: unknown consumer %q", consumer)
	}
	return store, nil
}
