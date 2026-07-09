# nutwords — real stats for a real app 🎲

Ripply pointed at the live [NutWords](https://github.com/kiliman) database
(Postgres on Neon): the online Scrabble app, ported from a 1999 ASP
classic/VB/SQL Server original. Replaces the hand-maintained `player_stats`
table with four incrementally-maintained indexes over the existing
`game_player` and `game_play` tables — **zero app changes, no rescans**.

```bash
bun --env-file=.env examples/nutwords/stats.ts           # build + leaderboards
bun --env-file=.env examples/nutwords/stats.ts --watch   # live: reprints as plays land
```

## Why this works with zero app changes

| App does (already) | Ripply sees |
|---|---|
| INSERT into `game_play` per move | new words/points/bingos tick in |
| UPDATE `game_player.result` at game end | seat starts counting (filtered map: `result == null → null`) |
| UPDATE `game_play.challenged_off` on a successful challenge | the play's whole contribution is **retracted** |

That last row is the one to appreciate: challenge reversal is just an UPDATE,
and reconcile-from-entries makes retraction automatic.

## The indexes

- `ripply_player_stats` — games / wins / points / hi + avg game score per player
- `ripply_play_stats` — turns, total points (incl. end-game bonus rows), highest turn
- `ripply_word_stats` — words, bingos, highest word score per player (multi-emit
  over the `words` jsonb; word text + game id ride along in entries for drill-down)
- `ripply_top_words` — hall of fame per word: times played, highest score

All four are real tables (`player_id` typed `integer` via `columnTypes`), so
leaderboards are plain SQL `JOIN player ON player.id = player_id`.

## What install touches

Everything Ripply creates is prefixed and reversible:

- `_ripply_capture` triggers on `game_player` / `game_play` (coexist with the
  app's `audit_*` triggers)
- `_ripply_changelog`, `_ripply_entries`, `_ripply_indexes` internal tables
- `ripply_player_stats`, `ripply_play_stats`, `ripply_word_stats`,
  `ripply_top_words` tally tables

Between runs the triggers keep capturing into `_ripply_changelog`; the next
run catches up and auto-prunes it. To uninstall completely:

```sql
DROP TRIGGER IF EXISTS _ripply_capture ON game_player;
DROP TRIGGER IF EXISTS _ripply_capture ON game_play;
DROP FUNCTION IF EXISTS _ripply_capture();
DROP TABLE IF EXISTS _ripply_changelog, _ripply_entries, _ripply_indexes,
  ripply_player_stats, ripply_play_stats, ripply_word_stats, ripply_top_words;
```

## Not yet: per-table-size stats

The legacy `player_stats` split stats by table size (2/3/4 players). That
lives on `game.max_players`, and Ripply maps see one row of one collection —
no joins (same rule as RavenDB's per-collection maps). When NutWords wants
the split back, denormalize `player_count` onto `game_player` at game start
(one line in the app), add it to `groupBy`, and Ripply's map-version rebuild
repopulates everything automatically.
