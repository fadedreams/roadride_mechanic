# Changelog

### Fixed
- **kafka**: batch outbox writes and offset commits to prevent rebalance loops under load ([8216934](https://github.com/fadedreams/roadride_mechanic/commit/8216934))
  - Replaced per-message synchronous Mongo transactions with size/time-bounded
    batching (100 messages or 2s linger, whichever comes first)
  - Existence checks and outbox inserts now run once per batch (single
    transaction) instead of once per message
  - Kafka offsets are committed once per batch (highest offset per partition)
    instead of after every message
  - Added per-batch processing time logging to replace the old per-message
    metric, tuned against the same `max.poll.interval.ms` canary threshold
  - Requires new `CheckOutboxEventsExist` and `SaveOutboxEvents` methods on
    `domain.MechanicRepository`

- **kafka**: prevent consumer group rebalance storms on restart ([27d46f4](https://github.com/fadedreams/roadride_mechanic/commit/27d46f4))
  - Added `group.instance.id` for static membership so pod/container restarts
    are treated as reconnects instead of leave+join
  - Set explicit `session.timeout.ms` (45s) and `max.poll.interval.ms` (5m)
  - Added per-message processing time logging to tune poll interval against real p99 data
  - Requires `INSTANCE_ID` env var at startup; fails fast if unset
