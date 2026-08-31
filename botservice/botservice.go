// Package botservice manages the lifecycle of a go-telegram/bot instance that
// runs over a SOCKS5 proxy: it builds the bot through the given builder, keeps
// the connection warm with a watchdog and restarts the bot with a new proxy
// when the current one becomes unreachable or stops delivering updates.
package botservice

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-telegram/bot"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"

	"github.com/azzimoda/go-tg-proxy/proxy"
)

// BotBuilderFunc builds a *bot.Bot for the given proxy address. onActivity is
// called on every incoming update and is used to detect a stalled connection.
type BotBuilderFunc = func(proxy string, onActivity func()) (*bot.Bot, error)

const (
	proxyWatchdogPeriod = 30 * time.Second
	proxyStallTimeout   = 3 * time.Minute
)

// NewBotService returns a BotService that builds bots via builder and rotates
// through the proxies managed by proxyService.
func NewBotService(builder BotBuilderFunc, proxyService *proxy.Service) *BotService {
	return &BotService{
		builder:     builder,
		proxy:       proxyService,
		restartChan: make(chan struct{}),
		restartSF:   &singleflight.Group{},
	}
}

type BotService struct {
	superCtx context.Context
	username string
	builder  BotBuilderFunc
	*bot.Bot
	proxy        *proxy.Service
	botCtx       context.Context
	botCtxCancel context.CancelFunc
	restartChan  chan struct{}
	onRestart    func(context.Context)
	restartSF    *singleflight.Group
	stopOnce     sync.Once
	stopMu       sync.Mutex
	stopped      bool
	activity     atomic.Int64
}

// Log returns the service logger carrying the bot username field.
func (s *BotService) Log() *zerolog.Logger {
	return new(log.Logger.With().Str("bot", s.username).Logger())
}

// Username returns the username of the currently running bot.
func (s *BotService) Username() string { return s.username }

// Touch records recent bot activity (an incoming update).
func (s *BotService) Touch() {
	s.activity.Store(time.Now().UnixNano())
}

// stalled reports whether no activity was seen for longer than proxyStallTimeout.
func (s *BotService) stalled(now time.Time) bool {
	return now.Sub(time.Unix(0, s.activity.Load())) > proxyStallTimeout
}

// HealthCheck returns nil when the running bot answers the Telegram API.
func (s *BotService) HealthCheck() error {
	s.stopMu.Lock()
	bot := s.Bot
	botCtx := s.botCtx
	s.stopMu.Unlock()

	if bot == nil {
		return nil
	}
	if _, err := bot.GetMe(botCtx); err != nil {
		return err
	}
	return nil
}

// Start runs the bot, restarting it with other proxies as needed, until ctx is
// cancelled or Stop is called.
func (s *BotService) Start(ctx context.Context) {
	s.superCtx = ctx

	s.stopMu.Lock()
	s.botCtx, s.botCtxCancel = context.WithCancel(ctx)
	botCtx := s.botCtx
	s.stopMu.Unlock()

	go s.startBot(ctx, botCtx)

	for {
		select {
		case <-ctx.Done():
			s.Log().Debug().Msg("Context cancelled, stopping bot")
			return
		case _, ok := <-s.restartChan:
			if !ok {
				s.Log().Error().Msg("Restart channel closed, stopping bot...")
				return
			}
			s.Log().Trace().Msg("Restarting bot...")
			s.stopMu.Lock()
			cancel := s.botCtxCancel
			s.botCtx, s.botCtxCancel = context.WithCancel(ctx)
			botCtx = s.botCtx
			s.Bot = nil
			s.stopMu.Unlock()

			cancel()
			s.startBot(ctx, botCtx)
		}
	}
}

// Restart signals the Start loop to restart the bot with a new proxy.
func (s *BotService) Restart() {
	s.restartSF.Do("restart", func() (any, error) {
		s.Log().Trace().Msg("Sending restart signal...")
		s.stopMu.Lock()
		if s.stopped {
			s.stopMu.Unlock()
			s.Log().Debug().Msg("Bot is stopped, skipping restart")
			return nil, nil
		}
		select {
		case s.restartChan <- struct{}{}:
			s.stopMu.Unlock()
			if s.onRestart != nil {
				s.onRestart(s.superCtx)
			}
		default:
			s.stopMu.Unlock()
			s.Log().Debug().Msg("Bot is not ready for restart, skipping")
		}
		return nil, nil
	})
}

// Stop stops the bot permanently.
func (s *BotService) Stop() {
	s.stopOnce.Do(func() {
		s.Log().Trace().Msg("Stopping bot...")
		s.stopMu.Lock()
		cancel := s.botCtxCancel
		s.stopped = true
		close(s.restartChan)
		s.stopMu.Unlock()

		if cancel != nil {
			cancel()
		}
	})
}

// OnRestart registers a callback invoked after a restart was signalled.
func (s *BotService) OnRestart(f func(context.Context)) { s.onRestart = f }

func (s *BotService) startBot(ctx context.Context, botCtx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		proxyAddr, err := s.proxy.FirstAvailable(ctx)
		if err != nil {
			s.Log().Error().Err(err).Msg("No available proxy, retrying in 10s...")
			time.Sleep(10 * time.Second)

			select {
			case <-ctx.Done():
				s.Log().Error().Msg("Context cancelled, stopping bot...")
				return
			default:
			}
			continue
		}
		s.Log().Debug().Str("proxy", proxyAddr)

		s.Log().Trace().Msg("Building bot...")
		b, err := s.builder(proxyAddr, s.Touch)
		if err != nil {
			s.Log().Error().Err(err).Msg("Failed to build bot, retrying in 5 second...")
			time.Sleep(5 * time.Second)
			continue
		}

		s.Log().Trace().Msg("Bot built, getting username...")
		if me, err := b.GetMe(ctx); err == nil {
			s.username = me.Username
			s.Log().Info().Msg("Bot started")
		} else {
			s.Log().Error().Err(err).Msg("Failed to get bot username, retrying in 5 seconds...")
			time.Sleep(5 * time.Second)
			continue
		}

		bot.WithErrorsHandler(func(err error) { go s.handleAPIError(err) })(b)
		s.Log().Trace().Msg("Bot initialized")

		s.Touch()
		s.stopMu.Lock()
		s.Bot = b
		bot := s.Bot
		s.stopMu.Unlock()

		go s.runWatchdog(botCtx, bot)

		bot.Start(botCtx)

		return
	}
}

// runWatchdog periodically probes Telegram and restarts the bot with a new
// proxy if the current one stopped delivering updates or became unreachable.
func (s *BotService) runWatchdog(botCtx context.Context, b *bot.Bot) {
	ticker := time.NewTicker(proxyWatchdogPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-botCtx.Done():
			return
		case <-ticker.C:
		}

		ctx, cancel := context.WithTimeout(botCtx, proxy.DefaultCheckTimeout)
		_, err := b.GetMe(ctx)
		cancel()
		if err != nil {
			s.Log().Error().Err(err).Msg("Watchdog: GetMe failed, restarting with a new proxy...")
			s.Restart()
			return
		}

		if s.stalled(time.Now()) {
			s.Log().Error().Msg("Watchdog: no updates for a long time, restarting with a new proxy...")
			s.Restart()
			return
		}
	}
}

func (s *BotService) handleAPIError(err error) {
	if isTelegramError(err) {
		s.Log().Warn().Err(err).Msg("Telegram API error, no restart needed")
		return
	}
	s.Log().Error().Err(err).Msg("Network error, restarting with a new proxy...")
	s.Restart()
}

func isTelegramError(err error) bool {
	return errors.Is(err, bot.ErrorNotFound) ||
		errors.Is(err, bot.ErrorConflict) ||
		errors.Is(err, bot.ErrorForbidden) ||
		errors.Is(err, bot.ErrorBadRequest) ||
		errors.Is(err, bot.ErrorUnauthorized) ||
		bot.IsTooManyRequestsError(err) ||
		bot.IsMigrateError(err)
}
