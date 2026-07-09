/**
 * NutWords player stats, powered by Ripply — no rescans, no manual stat
 * tables. Points at the live NutWords Postgres (Neon) via DATABASE_URL.
 *
 *   bun --env-file=.env examples/nutwords/stats.ts           # build + print
 *   bun --env-file=.env examples/nutwords/stats.ts --watch   # live leaderboards
 *
 * Four indexes over two collections:
 *
 *   player_stats  game_player  finished seats only (result != null):
 *                              games, wins, points, hi/avg game score
 *   play_stats    game_play    turns taken, total points (incl. end-game
 *                              bonus rows), highest single turn
 *   word_stats    game_play    MULTI-EMIT over the words jsonb: words
 *                              played, bingos, highest word score
 *   top_words     game_play    global hall of fame per WORD: times played,
 *                              highest score, bingo count
 *
 * The maps skip `challenged_off` plays — and because a successful challenge
 * UPDATEs that flag on the existing row, the trigger captures it and Ripply
 * retracts the play's entire contribution automatically. Same story when a
 * game finishes: the app's ordinary `result` UPDATE is the capture event.
 *
 * Every tally is a real table — the leaderboard queries below are plain SQL
 * JOINs against `player` for nicknames. See README.md for uninstall SQL.
 */

import { SQL } from 'bun';
import { createRipply } from '../../src/index';
import { postgresSource, postgresStore } from '../../src/postgres/index';

const url = process.env.DATABASE_URL;
if (!url) throw new Error('DATABASE_URL is not set (bun --env-file=.env …)');
const watch = process.argv.includes('--watch');

const sql = new SQL({ url, max: 4 });

const ripply = createRipply({
  source: postgresSource({
    sql,
    collections: {
      game_player: { pk: ['id'] },
      game_play: { pk: ['id'] },
    },
  }),
  store: postgresStore({ sql }),
  pollInterval: 1_000,
});

// --- row shapes (as to_jsonb renders them) ---------------------------------

interface GamePlayerRow {
  id: number;
  game_id: number;
  player_id: number;
  score: number;
  result: number | null; // null until the game finishes; 1 = winner
  [key: string]: unknown;
}

interface PlayWord {
  text: string;
  score: number;
  bingo: boolean;
}

interface GamePlayRow {
  id: number;
  game_id: number;
  player_id: number;
  action: string; // play | pass | exchange | challenge | bonus
  words: PlayWord[] | null;
  score_delta: number;
  challenged_off: boolean;
  [key: string]: unknown;
}

// --- indexes ----------------------------------------------------------------

ripply.defineIndex('player_stats', {
  collection: 'game_player',
  // finished seats only — when the app sets `result`, that UPDATE is the
  // capture event and the player's stats tick in the same drain
  map: (gp: GamePlayerRow) =>
    gp.result == null
      ? null
      : {
          player_id: gp.player_id,
          games: 1,
          wins: gp.result === 1 ? 1 : 0,
          score: gp.score,
        },
  reduce: {
    groupBy: ['player_id'],
    aggregate: {
      games: { sum: 'games' },
      wins: { sum: 'wins' },
      points: { sum: 'score' },
      hi_game_score: { max: 'score' },
      avg_game_score: { avg: 'score' },
    },
  },
  columnTypes: { player_id: 'integer' }, // joinable against player.id
  indexes: [['wins'], ['hi_game_score']],
});

ripply.defineIndex('play_stats', {
  collection: 'game_play',
  map: (p: GamePlayRow) =>
    p.challenged_off
      ? null
      : {
          player_id: p.player_id,
          turn: p.action === 'play' || p.action === 'pass' || p.action === 'exchange' ? 1 : 0,
          points: p.score_delta, // bonus rows included → matches final scores
          turn_score: p.action === 'play' ? p.score_delta : null, // null → skipped by max
        },
  reduce: {
    groupBy: ['player_id'],
    aggregate: {
      turns: { sum: 'turn' },
      points: 'sum',
      hi_turn_score: { max: 'turn_score' },
    },
  },
  columnTypes: { player_id: 'integer' },
});

const wordStats = ripply.defineIndex('word_stats', {
  collection: 'game_play',
  // multi-emit: one entry per word formed this turn (main + auxiliaries);
  // `text`/`game_id` ride along in the entries for drill-down
  map: (p: GamePlayRow) =>
    !p.words || p.challenged_off
      ? null
      : p.words.map((w) => ({
          player_id: p.player_id,
          text: w.text,
          game_id: p.game_id,
          word: 1,
          bingo: w.bingo ? 1 : 0,
          score: w.score,
        })),
  reduce: {
    groupBy: ['player_id'],
    aggregate: {
      words: { sum: 'word' },
      bingos: { sum: 'bingo' },
      hi_word_score: { max: 'score' },
    },
  },
  columnTypes: { player_id: 'integer' },
});

ripply.defineIndex('top_words', {
  collection: 'game_play',
  map: (p: GamePlayRow) =>
    !p.words || p.challenged_off
      ? null
      : p.words.map((w) => ({
          word: w.text,
          plays: 1,
          score: w.score,
          bingo: w.bingo ? 1 : 0,
        })),
  reduce: {
    groupBy: ['word'],
    aggregate: {
      plays: { sum: 'plays' },
      hi_score: { max: 'score' },
      bingos: { sum: 'bingo' },
    },
  },
  indexes: [['hi_score']],
});

// --- leaderboards: plain SQL against the tally tables ------------------------

async function table(label: string, query: string) {
  const rows = (await sql.unsafe(query)) as Array<Record<string, unknown>>;
  console.log(`\n${label}`);
  console.table(rows);
}

async function printLeaderboards() {
  await table(
    '🏆 Standings (finished games)',
    `SELECT p.nickname, s.games, s.wins, s.points,
            s.hi_game_score AS hi_game, round(s.avg_game_score::numeric, 1) AS avg_game
     FROM ripply_player_stats s JOIN player p ON p.id = s.player_id
     ORDER BY s.wins DESC, s.games DESC LIMIT 10`,
  );
  await table(
    '🎯 Words & bingos (all plays)',
    `SELECT p.nickname, w.words, w.bingos, w.hi_word_score AS hi_word,
            t.turns, t.hi_turn_score AS hi_turn
     FROM ripply_word_stats w
     JOIN ripply_play_stats t USING (player_id)
     JOIN player p ON p.id = w.player_id
     ORDER BY w.bingos DESC, w.words DESC LIMIT 10`,
  );
  await table(
    '📜 Hall of fame — highest-scoring words',
    `SELECT word, hi_score, plays, bingos
     FROM ripply_top_words ORDER BY hi_score DESC LIMIT 10`,
  );

  // drill-down: WHICH word was the top scorer's best (entries, not SQL)
  const [top] = (await sql.unsafe(
    `SELECT s.player_id, p.nickname, s.hi_word_score
     FROM ripply_word_stats s JOIN player p ON p.id = s.player_id
     ORDER BY s.hi_word_score DESC LIMIT 1`,
  )) as Array<{ player_id: number; nickname: string; hi_word_score: number }>;
  if (top) {
    const entries = await wordStats.where({ player_id: top.player_id }).entries();
    const best = entries.find((e) => e.entry.score === top.hi_word_score);
    console.log(
      `\n💎 Best word on record: "${best?.entry.text}" — ${top.hi_word_score} pts by ${top.nickname}` +
        ` (game ${best?.entry.game_id})`,
    );
  }
}

// --- run ---------------------------------------------------------------------

const ms = (from: number) => `${Math.round(performance.now() - from)}ms`;
const t0 = performance.now();
await ripply.start(); // installs capture, builds/catches up all four indexes
console.log(`start(): ${ms(t0)}`);
const t1 = performance.now();
await ripply.drain();
console.log(`drain(): ${ms(t1)} — capture installed, indexes fresh`);
await printLeaderboards();

if (watch) {
  console.log('\n👀 watching for plays (Ctrl-C to stop)…');
  // ripply's background poller keeps the indexes fresh; we reprint on change
  setInterval(() => {
    void ripply.drain().then(async (changes) => {
      if (changes > 0) {
        console.log(`\n🌊 ${changes} change(s) rippled through`);
        await printLeaderboards();
      }
    });
  }, 3_000);
} else {
  await ripply.stop();
  await sql.close();
}
