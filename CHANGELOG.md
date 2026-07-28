# Changelog

## [Unreleased]

### Fixed
- **kafka**: prevent consumer group rebalance storms on restart ([27d46f4](https://github.com/fadedreams/roadride_mechanic/commit/27d46f4))
  - Added `group.instance.id` for static membership so pod/container restarts
    are treated as reconnects instead of leave+join
  - Set explicit `session.timeout.ms` (45s) and `max.poll.interval.ms` (5m)
  - Added per-message processing time logging to tune poll interval against real p99 data
  - Requires `INSTANCE_ID` env var at startup; fails fast if unset
