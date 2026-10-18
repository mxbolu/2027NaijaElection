import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { basicAuth } from "hono/basic-auth";
import { streamSSE } from "hono/streaming";
import pg from "pg";
import { pool, migrate } from "./db.js";

if (!process.env.ADMIN_PASSWORD) throw new Error("ADMIN_PASSWORD is required");

const app = new Hono();

// One LISTEN connection per replica fans out publish events to all SSE viewers.
// Postgres NOTIFY keeps viewers in sync across replicas.
const subscribers = new Set<() => void>();
const listener = new pg.Client({ connectionString: process.env.DATABASE_URL });
listener.on("error", (err) => {
  console.error("listener error", err);
  process.exit(1); // Railway restarts the service
});
listener.on("notification", () => subscribers.forEach((notify) => notify()));

const esc = (s: unknown) =>
  String(s).replace(/[&<>"']/g, (ch) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" } as Record<string, string>
  )[ch]);

const resultsSql = `
  SELECT c.name AS constituency, ca.id AS candidate_id, ca.name, ca.party,
         COALESCE(r.published_votes, 0) AS votes
  FROM candidates ca
  JOIN constituencies c ON c.id = ca.constituency_id
  LEFT JOIN results r ON r.candidate_id = ca.id
  ORDER BY c.name, votes DESC, ca.name`;

const audit = (db: pg.Pool | pg.PoolClient, action: string, detail: string) =>
  db.query("INSERT INTO audit_log (action, actor, detail) VALUES ($1, 'admin', $2)", [action, detail]);

// Public
app.get("/healthz", (c) => c.text("ok"));

app.get("/api/results", async (c) => c.json((await pool.query(resultsSql)).rows));

app.get("/api/events", (c) =>
  streamSSE(c, async (stream) => {
    const notify = () => {
      stream.writeSSE({ event: "update", data: "published" }).catch(() => {});
    };
    subscribers.add(notify);
    stream.onAbort(() => { subscribers.delete(notify); });
    while (!stream.aborted) {
      await stream.writeSSE({ event: "ping", data: "" });
      await stream.sleep(25_000);
    }
  }),
);

app.get("/", (c) =>
  c.html(`<!doctype html>
<html><head><meta charset="utf-8"><title>2027 Election Results</title>
<style>body{font-family:system-ui;margin:2rem;max-width:900px}table{border-collapse:collapse;width:100%}td,th{padding:.4rem;border-bottom:1px solid #ddd;text-align:left}h2{margin-top:2rem}</style>
</head><body>
<h1>2027 Election Results</h1><p id="status">Loading...</p><div id="out"></div>
<script>
const esc = s => String(s).replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
async function load() {
  const rows = await (await fetch('/api/results')).json();
  const groups = {};
  for (const r of rows) (groups[r.constituency] ??= []).push(r);
  document.getElementById('out').innerHTML = Object.entries(groups).map(([name, cands]) =>
    '<h2>' + esc(name) + '</h2><table><tr><th>Candidate</th><th>Party</th><th>Votes</th></tr>' +
    cands.map(c => '<tr><td>' + esc(c.name) + '</td><td>' + esc(c.party) + '</td><td>' +
      Number(c.votes).toLocaleString() + '</td></tr>').join('') + '</table>').join('');
  document.getElementById('status').textContent = 'Live. Last updated ' + new Date().toLocaleTimeString();
}
load();
new EventSource('/api/events').addEventListener('update', load);
</script></body></html>`),
);

// Admin (basic auth: user "admin", password from ADMIN_PASSWORD)
const auth = basicAuth({ username: "admin", password: process.env.ADMIN_PASSWORD! });
app.use("/admin", auth);
app.use("/admin/*", auth);

app.get("/admin", async (c) => {
  const { rows } = await pool.query(`
    SELECT ca.id, c.name AS constituency, ca.name, ca.party,
           COALESCE(r.draft_votes, 0) AS draft, COALESCE(r.published_votes, 0) AS published
    FROM candidates ca
    JOIN constituencies c ON c.id = ca.constituency_id
    LEFT JOIN results r ON r.candidate_id = ca.id
    ORDER BY c.name, ca.name`);
  const body = rows.map((r) => `
    <tr><td>${esc(r.constituency)}</td><td>${esc(r.name)} (${esc(r.party)})</td><td>${r.published}</td>
    <td><form method="post" action="/admin/draft">
      <input type="hidden" name="candidate_id" value="${r.id}">
      <input name="votes" type="number" min="0" value="${r.draft}" style="width:8rem">
      <button>Save draft</button></form></td></tr>`).join("");
  return c.html(`<!doctype html>
<html><head><meta charset="utf-8"><title>Admin</title></head>
<body style="font-family:system-ui;margin:2rem">
<h1>Admin</h1>
<h3>Add candidate</h3>
<form method="post" action="/admin/candidate">
  <input name="constituency" placeholder="Constituency" required>
  <input name="name" placeholder="Candidate name" required>
  <input name="party" placeholder="Party" required>
  <button>Add</button>
</form>
<h3>Results (published vs draft)</h3>
<table border="1" cellpadding="6">
  <tr><th>Constituency</th><th>Candidate</th><th>Published</th><th>Draft votes</th></tr>${body}
</table>
<h3>Publish</h3>
<form method="post" action="/admin/publish" onsubmit="return confirm('Publish all draft results?')">
  <button>Publish all drafts</button>
</form>
</body></html>`);
});

app.post("/admin/candidate", async (c) => {
  const f = await c.req.parseBody();
  const constituency = String(f.constituency ?? "").trim();
  const name = String(f.name ?? "").trim();
  const party = String(f.party ?? "").trim();
  if (!constituency || !name || !party) return c.text("All fields are required", 400);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query(
      `INSERT INTO constituencies (name) VALUES ($1)
       ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id`,
      [constituency],
    );
    await client.query(
      "INSERT INTO candidates (constituency_id, name, party) VALUES ($1, $2, $3)",
      [rows[0].id, name, party],
    );
    await audit(client, "add_candidate", `${constituency}: ${name} (${party})`);
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
  return c.redirect("/admin");
});

app.post("/admin/draft", async (c) => {
  const f = await c.req.parseBody();
  const candidateId = Number(f.candidate_id);
  const votes = Number(f.votes);
  if (!Number.isInteger(candidateId) || !Number.isInteger(votes) || votes < 0) {
    return c.text("Invalid input", 400);
  }
  await pool.query(
    `INSERT INTO results (candidate_id, draft_votes) VALUES ($1, $2)
     ON CONFLICT (candidate_id) DO UPDATE SET draft_votes = EXCLUDED.draft_votes, updated_at = now()`,
    [candidateId, votes],
  );
  await audit(pool, "save_draft", `candidate ${candidateId} = ${votes}`);
  return c.redirect("/admin");
});

app.post("/admin/publish", async (c) => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { rowCount } = await client.query(
      "UPDATE results SET published_votes = draft_votes, updated_at = now()",
    );
    await audit(client, "publish", `${rowCount ?? 0} result rows`);
    await client.query("NOTIFY results_published, 'publish'");
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
  return c.redirect("/admin");
});

app.onError((err, c) => {
  console.error(err);
  return c.text("Server error", 500);
});

await migrate();
await listener.connect();
await listener.query("LISTEN results_published");

const port = Number(process.env.PORT ?? 3000);
serve({ fetch: app.fetch, port }, (info) => console.log(`listening on ${info.port}`));

