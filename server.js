"use strict";

const express = require("express");
const Database = require("better-sqlite3");
const path = require("path");

const PORT = 3001;
const DB_PATH = path.join(__dirname, "data.sqlite");

const ON_TIME_THRESHOLD = 1800; // задержка <= 30 мин = "отправлено вовремя"
const DELAY_CAP = 86400; // потолок учитываемой задержки, 24 ч
const DELAY_REF = 3600; // масштаб задержки для признака low_delay, 1 ч
const EWMA_ALPHA = 0.2; // сглаживание, память ~5 дней при 1 запросе/сутки
const MIN_SAMPLES = 4;
const MIN_UPTIME_DAYS = 1;

// p = sigmoid(bias + Σ wᵢ·xᵢ); признаки в [0,1], где 1 = похоже на ВМ
const MODEL = { bias: -4, w_on_time: 5, w_low_delay: 2, w_spread: 1 };
const HOUR_ONLY = { bias: -2, w_spread: 4 }; // запасной режим без scheduled_ts

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");

const CURRENT_SCHEMA = {
  hwid: "TEXT PRIMARY KEY",
  first_seen: "INTEGER NOT NULL",
  last_seen: "INTEGER NOT NULL",
  query_count: "INTEGER NOT NULL DEFAULT 0",
  delay_samples: "INTEGER NOT NULL DEFAULT 0",
  on_time_count: "INTEGER NOT NULL DEFAULT 0",
  delayed_count: "INTEGER NOT NULL DEFAULT 0",
  ewma_on_time: "REAL NOT NULL DEFAULT 0",
  ewma_delay: "REAL NOT NULL DEFAULT 0",
  max_delay: "INTEGER NOT NULL DEFAULT 0",
  hour_mask: "INTEGER NOT NULL DEFAULT 0",
  hour_cos_sum: "REAL NOT NULL DEFAULT 0",
  hour_sin_sum: "REAL NOT NULL DEFAULT 0",
};

// миграция: переносим совпадающие столбцы, новые заполняем дефолтами
function migrate() {
  const cols = `(${Object.entries(CURRENT_SCHEMA)
    .map(([k, t]) => `${k} ${t}`)
    .join(", ")})`;
  const existing = db.prepare("PRAGMA table_info(bots)").all();
  console.log(existing);

  if (existing.length === 0) {
    db.exec(`CREATE TABLE bots ${cols};`);
    return;
  }

  const have = new Set(existing.map((c) => c.name));
  const want = Object.keys(CURRENT_SCHEMA);
  const upToDate =
    want.every((c) => have.has(c)) && existing.length === want.length;
  if (upToDate) return;

  const shared = want.filter((c) => have.has(c));
  db.exec("BEGIN");
  try {
    db.exec(`CREATE TABLE bots_new ${cols};`);
    if (shared.length) {
      db.exec(
        `INSERT INTO bots_new (${shared.join(", ")}) SELECT ${shared.join(", ")} FROM bots;`,
      );
    }
    db.exec("DROP TABLE bots;");
    db.exec("ALTER TABLE bots_new RENAME TO bots;");
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }
}
migrate();

const selectBot = db.prepare("SELECT * FROM bots WHERE hwid = ?");

const insertBot = db.prepare(`
  INSERT INTO bots (
    hwid, first_seen, last_seen, query_count,
    delay_samples, on_time_count, delayed_count,
    ewma_on_time, ewma_delay, max_delay,
    hour_mask, hour_cos_sum, hour_sin_sum
  ) VALUES (
    @hwid, @first_seen, @last_seen, @query_count,
    @delay_samples, @on_time_count, @delayed_count,
    @ewma_on_time, @ewma_delay, @max_delay,
    @hour_mask, @hour_cos_sum, @hour_sin_sum
  )
`);

const updateBot = db.prepare(`
  UPDATE bots SET
    last_seen     = @last_seen,
    query_count   = @query_count,
    delay_samples = @delay_samples,
    on_time_count = @on_time_count,
    delayed_count = @delayed_count,
    ewma_on_time  = @ewma_on_time,
    ewma_delay    = @ewma_delay,
    max_delay     = @max_delay,
    hour_mask     = @hour_mask,
    hour_cos_sum  = @hour_cos_sum,
    hour_sin_sum  = @hour_sin_sum
  WHERE hwid = @hwid
`);

// чтение→расчёт→запись в одной транзакции (защита от гонок по одному HWID)
const recordRequest = db.transaction((hwid, scheduledTs, actualTs, now) => {
  const hour = new Date(actualTs * 1000).getUTCHours();
  const theta = (hour / 24) * 2 * Math.PI;
  const cos = Math.cos(theta);
  const sin = Math.sin(theta);

  const hasDelay = Number.isFinite(scheduledTs);
  const delay = hasDelay
    ? Math.min(Math.max(0, actualTs - scheduledTs), DELAY_CAP)
    : 0;
  const onTime = delay <= ON_TIME_THRESHOLD ? 1 : 0;

  const existing = selectBot.get(hwid);

  if (!existing) {
    const row = {
      hwid,
      first_seen: now,
      last_seen: now,
      query_count: 1,
      delay_samples: hasDelay ? 1 : 0,
      on_time_count: hasDelay ? onTime : 0,
      delayed_count: hasDelay ? 1 - onTime : 0,
      ewma_on_time: hasDelay ? onTime : 0,
      ewma_delay: hasDelay ? delay : 0,
      max_delay: hasDelay ? delay : 0,
      hour_mask: 1 << hour,
      hour_cos_sum: cos,
      hour_sin_sum: sin,
    };
    insertBot.run(row);
    return row;
  }

  let {
    delay_samples,
    on_time_count,
    delayed_count,
    ewma_on_time,
    ewma_delay,
    max_delay,
  } = existing;
  if (hasDelay) {
    if (delay_samples === 0) {
      ewma_on_time = onTime; // первый замер задаёт начальные значения EWMA
      ewma_delay = delay;
    } else {
      ewma_on_time = EWMA_ALPHA * onTime + (1 - EWMA_ALPHA) * ewma_on_time;
      ewma_delay = EWMA_ALPHA * delay + (1 - EWMA_ALPHA) * ewma_delay;
    }
    delay_samples += 1;
    on_time_count += onTime;
    delayed_count += 1 - onTime;
    if (delay > max_delay) max_delay = delay;
  }

  const row = {
    hwid,
    first_seen: existing.first_seen,
    last_seen: now,
    query_count: existing.query_count + 1,
    delay_samples,
    on_time_count,
    delayed_count,
    ewma_on_time,
    ewma_delay,
    max_delay,
    hour_mask: existing.hour_mask | (1 << hour),
    hour_cos_sum: existing.hour_cos_sum + cos,
    hour_sin_sum: existing.hour_sin_sum + sin,
  };
  updateBot.run(row);
  return row;
});

function popcount24(mask) {
  let m = mask & 0xffffff;
  let c = 0;
  while (m) {
    m &= m - 1;
    c++;
  }
  return c;
}
const sigmoid = (z) => 1 / (1 + Math.exp(-z));
const clamp01 = (x) => (x < 0 ? 0 : x > 1 ? 1 : x);

function classify(row) {
  const n = row.query_count;
  const uptimeDays = (row.last_seen - row.first_seen) / 86400;

  if (n < MIN_SAMPLES || uptimeDays < MIN_UPTIME_DAYS) {
    return {
      res: "no",
      score: 0,
      probability: 0,
      reason: "недостаточно данных",
    };
  }

  // кольцевая концентрация активности по часам: R≈1 — собрана, R≈0 — размазана
  const meanCos = row.hour_cos_sum / n;
  const meanSin = row.hour_sin_sum / n;
  const R = Math.sqrt(meanCos * meanCos + meanSin * meanSin);
  const f = { spread: clamp01(1 - R) };

  let z;
  let mode;
  if (row.delay_samples >= 1) {
    mode = "delay";
    f.on_time = clamp01(row.ewma_on_time);
    f.low_delay = clamp01(1 - row.ewma_delay / DELAY_REF);
    z =
      MODEL.bias +
      MODEL.w_on_time * f.on_time +
      MODEL.w_low_delay * f.low_delay +
      MODEL.w_spread * f.spread;
  } else {
    mode = "hour-only"; // нет scheduled_ts — слабый режим только по часам
    z = HOUR_ONLY.bias + HOUR_ONLY.w_spread * f.spread;
  }

  const probability = sigmoid(z);

  const details = {
    mode,
    uptime_days: Number(uptimeDays.toFixed(2)),
    query_count: n,
    delay_samples: row.delay_samples,
    on_time_count: row.on_time_count,
    delayed_count: row.delayed_count,
    on_time_ratio: row.delay_samples
      ? Number((row.on_time_count / row.delay_samples).toFixed(3))
      : null,
    ewma_delay_sec: Number(row.ewma_delay.toFixed(1)),
    max_delay_sec: row.max_delay,
    active_hours: popcount24(row.hour_mask),
    concentration_R: Number(R.toFixed(3)),
    features: Object.fromEntries(
      Object.entries(f).map(([k, v]) => [k, Number(v.toFixed(3))]),
    ),
  };

  return {
    res: probability >= 0.5 ? "yes" : "no",
    score: Number(z.toFixed(3)),
    probability: Number(probability.toFixed(4)),
    details,
  };
}

// мемоизация по (hwid → query_count): тот же стейт не пересчитываем
const classifyCache = new Map();
function classifyCached(row) {
  const cached = classifyCache.get(row.hwid);
  if (cached && cached.count === row.query_count) return cached.decision;
  const decision = classify(row);
  classifyCache.set(row.hwid, { count: row.query_count, decision });
  return decision;
}

const app = express();
app.use(express.json());

function toUnix(v) {
  const n = typeof v === "string" ? Number(v) : v;
  return Number.isFinite(n) ? Math.floor(n) : undefined;
}

function handle(req, res) {
  const src = { ...req.query, ...req.body };
  const hwid = src.hwid;
  if (!hwid || typeof hwid !== "string") {
    return res.status(400).json({ error: "hwid is required (string)" });
  }

  const now = Math.floor(Date.now() / 1000);
  const scheduledTs = toUnix(src.scheduled_ts);
  const actualTs = toUnix(src.actual_ts) ?? now; // факт с часов клиента, иначе серверное время

  const row = recordRequest(hwid, scheduledTs, actualTs, now);
  const decision = classifyCached(row);

  return res.json({ res: decision.res });
}

app.post("/check", handle);
app.get("/check", handle);

app.get("/stats/:hwid", (req, res) => {
  const row = selectBot.get(req.params.hwid);
  if (!row) return res.status(404).json({ error: "unknown hwid" });
  return res.json({ row, decision: classify(row) });
});

app.get("/", (_req, res) => {
  res.json({
    service: "vm-detector",
    endpoints: ["POST /check", "GET /check?hwid=", "GET /stats/:hwid"],
    body: {
      hwid: "string",
      scheduled_ts: "unix sec (опц.)",
      actual_ts: "unix sec (опц.)",
    },
  });
});

app.listen(PORT, () => {
  console.log(`VM-detector backend listening on http://localhost:${PORT}`);
});
