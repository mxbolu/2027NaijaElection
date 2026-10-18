# 2027 Naija Elections

Live election results: public read-only page with live updates, plus an admin area to enter draft results and publish them.

## Stack
- Node.js + TypeScript, Hono, PostgreSQL (`pg`)
- Live updates via Server-Sent Events, fed by Postgres `LISTEN/NOTIFY` (works across replicas)

## Environment variables
- `DATABASE_URL` - Postgres connection string (reference the Postgres service)
- `ADMIN_PASSWORD` - password for the `/admin` area (user: `admin`)
- `PORT` - set by Railway

## Routes
- `/` - public results page
- `/api/results` - published results as JSON
- `/api/events` - SSE stream, emits `update` on each publish
- `/admin` - add candidates, enter draft votes, publish
- `/healthz` - health check

## Local development
```bash
npm install
DATABASE_URL=postgres://... ADMIN_PASSWORD=changeme npm run dev
```

