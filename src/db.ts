import pg from "pg";

export const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

export async function migrate() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS constituencies (
      id   SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE
    );
    CREATE TABLE IF NOT EXISTS candidates (
      id              SERIAL PRIMARY KEY,
      constituency_id INT  NOT NULL REFERENCES constituencies(id),
      name            TEXT NOT NULL,
      party           TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS results (
      candidate_id    INT PRIMARY KEY REFERENCES candidates(id),
      draft_votes     INT NOT NULL DEFAULT 0,
      published_votes INT NOT NULL DEFAULT 0,
      updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS audit_log (
      id         SERIAL PRIMARY KEY,
      action     TEXT NOT NULL,
      actor      TEXT NOT NULL,
      detail     TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
}

