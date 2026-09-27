package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog"

	"github.com/hatchet-dev/hatchet/internal/datautils"
	"github.com/hatchet-dev/hatchet/internal/integrations/alerting"
	"github.com/hatchet-dev/hatchet/internal/services/partition"
	hatcheterrors "github.com/hatchet-dev/hatchet/pkg/errors"
	"github.com/hatchet-dev/hatchet/pkg/logger"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
)

type RetentionController interface {
	Start(ctx context.Context) error
}

type RetentionControllerImpl struct {
	l                    *zerolog.Logger
	repo                 v1.Repository
	dv                   datautils.DataDecoderValidator
	s                    gocron.Scheduler
	tenantAlerter        *alerting.TenantAlertManager
	a                    *hatcheterrors.Wrapped
	p                    *partition.Partition
	dataRetention        bool
	workerRetention      bool
	queueRetention       bool
	userSessionRetention bool

	softDeleteTenantReap         bool
	softDeleteTenantReapGrace    time.Duration
	softDeleteTenantReapInterval string
}

type RetentionControllerOpt func(*RetentionControllerOpts)

type RetentionControllerOpts struct {
	l                    *zerolog.Logger
	repo                 v1.Repository
	dv                   datautils.DataDecoderValidator
	ta                   *alerting.TenantAlertManager
	alerter              hatcheterrors.Alerter
	p                    *partition.Partition
	dataRetention        bool
	workerRetention      bool
	queueRetention       bool
	userSessionRetention bool

	softDeleteTenantReap         bool
	softDeleteTenantReapGrace    time.Duration
	softDeleteTenantReapInterval string
}

func defaultRetentionControllerOpts() *RetentionControllerOpts {
	logger := logger.NewDefaultLogger("retention-controller")
	alerter := hatcheterrors.NoOpAlerter{}

	return &RetentionControllerOpts{
		l:                    &logger,
		dv:                   datautils.NewDataDecoderValidator(),
		alerter:              alerter,
		dataRetention:        true,
		queueRetention:       true,
		workerRetention:      false,
		userSessionRetention: true,
	}
}

func WithLogger(l *zerolog.Logger) RetentionControllerOpt {
	return func(opts *RetentionControllerOpts) {
		opts.l = l
	}
}

func WithRepository(r v1.Repository) RetentionControllerOpt {
	return func(opts *RetentionControllerOpts) {
		opts.repo = r
	}
}

func WithAlerter(a hatcheterrors.Alerter) RetentionControllerOpt {
	return func(opts *RetentionControllerOpts) {
		opts.alerter = a
	}
}

func WithDataDecoderValidator(dv datautils.DataDecoderValidator) RetentionControllerOpt {
	return func(opts *RetentionControllerOpts) {
		opts.dv = dv
	}
}

func WithTenantAlerter(ta *alerting.TenantAlertManager) RetentionControllerOpt {
	return func(opts *RetentionControllerOpts) {
		opts.ta = ta
	}
}

func WithPartition(p *partition.Partition) RetentionControllerOpt {
	return func(opts *RetentionControllerOpts) {
		opts.p = p
	}
}

func WithDataRetention(b bool) RetentionControllerOpt {
	return func(opts *RetentionControllerOpts) {
		opts.dataRetention = b
	}
}

func WithWorkerRetention(b bool) RetentionControllerOpt {
	return func(opts *RetentionControllerOpts) {
		opts.workerRetention = b
	}
}

// WithSoftDeleteTenantReap enables the periodic hard deletion of
// soft-deleted tenants whose grace period has elapsed. grace is the undo
// window after a soft delete before the tenant's rows are physically
// removed; interval is how often the reap pass runs (a Go duration string).
func WithSoftDeleteTenantReap(enabled bool, grace time.Duration, interval string) RetentionControllerOpt {
	return func(opts *RetentionControllerOpts) {
		opts.softDeleteTenantReap = enabled
		opts.softDeleteTenantReapGrace = grace
		opts.softDeleteTenantReapInterval = interval
	}
}

func New(fs ...RetentionControllerOpt) (*RetentionControllerImpl, error) {
	opts := defaultRetentionControllerOpts()

	for _, f := range fs {
		f(opts)
	}

	if opts.repo == nil {
		return nil, fmt.Errorf("repository is required. use WithRepository")
	}

	if opts.ta == nil {
		return nil, fmt.Errorf("tenant alerter is required. use WithTenantAlerter")
	}

	if opts.p == nil {
		return nil, fmt.Errorf("partition is required. use WithPartition")
	}

	s, err := gocron.NewScheduler(gocron.WithLocation(time.UTC))

	if err != nil {
		return nil, fmt.Errorf("could not create scheduler: %w", err)
	}

	newLogger := opts.l.With().Str("service", "retention-controller").Logger()
	opts.l = &newLogger

	a := hatcheterrors.NewWrapped(opts.alerter)
	a.WithData(map[string]interface{}{"service": "retention-controller"})

	return &RetentionControllerImpl{
		l:                    opts.l,
		repo:                 opts.repo,
		dv:                   opts.dv,
		s:                    s,
		tenantAlerter:        opts.ta,
		a:                    a,
		p:                    opts.p,
		dataRetention:        opts.dataRetention,
		workerRetention:      opts.workerRetention,
		queueRetention:       opts.queueRetention,
		userSessionRetention: opts.userSessionRetention,

		softDeleteTenantReap:         opts.softDeleteTenantReap,
		softDeleteTenantReapGrace:    opts.softDeleteTenantReapGrace,
		softDeleteTenantReapInterval: opts.softDeleteTenantReapInterval,
	}, nil
}

func (rc *RetentionControllerImpl) Start() (func() error, error) {
	rc.l.Debug().Msg("starting retention controller")

	var err error
	ctx, cancel := context.WithCancelCause(context.Background())
	defer func() {
		if err != nil {
			cancel(err)
		}
	}()

	if rc.queueRetention {
		queueInterval := time.Second * 60

		_, err = rc.s.NewJob(
			gocron.DurationJob(queueInterval),
			gocron.NewTask(
				rc.runDeleteMessageQueueItems(ctx),
			),
		)

		if err != nil {
			return nil, fmt.Errorf("could not set up runDeleteMessageQueueItems: %w", err)
		}
	}

	if rc.workerRetention {
		workerInterval := 24 * time.Hour

		_, err = rc.s.NewJob(
			gocron.DurationJob(workerInterval),
			gocron.NewTask(
				rc.runCleanupOldWorkers(ctx),
			),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		)

		if err != nil {
			return nil, fmt.Errorf("could not set up runCleanupOldWorkers: %w", err)
		}
	}

	if rc.userSessionRetention {
		userSessionInterval := 1 * time.Hour

		_, err = rc.s.NewJob(
			gocron.DurationJob(userSessionInterval),
			gocron.NewTask(
				rc.runCleanupUserSessions(ctx),
			),
		)

		if err != nil {
			return nil, fmt.Errorf("could not set up runCleanupUserSessions: %w", err)
		}
	}

	if rc.softDeleteTenantReap {
		reapInterval, err := time.ParseDuration(rc.softDeleteTenantReapInterval)
		if err != nil {
			return nil, fmt.Errorf("invalid soft-delete tenant reap interval %q: %w", rc.softDeleteTenantReapInterval, err)
		}

		_, err = rc.s.NewJob(
			gocron.DurationJob(reapInterval),
			gocron.NewTask(
				rc.runReapSoftDeletedTenants(ctx),
			),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		)

		if err != nil {
			return nil, fmt.Errorf("could not set up runReapSoftDeletedTenants: %w", err)
		}
	}

	rc.s.Start()

	cleanup := func() error {
		cancel(nil)

		if err := rc.s.Shutdown(); err != nil {
			return fmt.Errorf("could not shutdown scheduler: %w", err)
		}

		return nil
	}

	return cleanup, nil
}
