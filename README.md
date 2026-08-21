<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./assets/resonate-banner.png">
  <img alt="Resonate" src="./assets/resonate-banner-light.png">
</picture>

# Resonate on Postgres

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## About this component

**Dead simple durable execution.**

Resonate durable execution runs your code as a reliable workflow, checkpointing each step as a durable promise, sleeping for days, surviving crashes and restarts. resonate-pg is one SQL file. No additional servers, queues, or timers — Postgres, with pg_cron, is all three. Full Resonate docs live at [docs.resonatehq.io](https://docs.resonatehq.io).

```ts
resonate.register(
  "countdown",
  async function countdown(ctx: Context, n: number) {
    for (let i = n; i > 0; i--) {
      await ctx.run(() => console.log(`countdown: ${i}`));
      await ctx.sleep(10 * 60 * 1000); // durable: no process waits
    }
    await ctx.run(() => console.log("liftoff 🚀"));
  },
);
``` 

Crash the process, redeploy, or lose the machine mid-run — the workflow resumes on the right number, and nothing runs twice. Each `ctx.run` is checkpointed to Postgres as it completes, so a resumed run replays finished steps from the database instead of re-executing them. And `ctx.sleep` is just a row with a deadline: the invocation returns and nothing runs until it fires, whether that's ten minutes or ten days from now.

> **On Supabase?** [`example/countdown`](example/countdown) goes from an empty project to a running durable workflow in about five minutes.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./assets/quickstart-banner.png">
  <img alt="Quickstart" src="./assets/quickstart-banner-light.png">
</picture>

## Requirements

- Postgres 16+
- `pg_cron` — required; it drives all timers
- `pg_net` or `pgsql_http` — optional; either one enables HTTP push delivery

## Install

resonate-pg is a Resonate server in a single file. Apply it to a database:

```bash
psql -d yourdb -f resonate.sql
```

There is no binary to run and no process to supervise. Postgres is the server.

## Connecting workers

Every protocol action is a stored procedure, reached through the `resonate_rpc` dispatcher rather than an HTTP URL:

```sql
SELECT resonate.resonate_rpc('{"kind":"promise.get","head":{},"data":{"id":"invoke:foo"}}');
```

So a worker needs a client that speaks to that function over a Postgres connection. The path demonstrated in this repository is the Supabase TypeScript client:

```typescript
import { type Context, Resonate } from "jsr:@resonatehq/supabase@0.4.1";

const resonate = new Resonate();
```

Both examples under [`example/`](example) use it. See [`example/countdown`](example/countdown) for a full deployment, empty project to running workflow.

`test/conformance.py` shows the other shape: a shim that puts an HTTP interface in front of `resonate_rpc`. It exists to let the conformance harness drive the database and is not a shipped client, but it is the pattern an HTTP-based SDK client would need.

## Operations

Completed workflows stay in the database. Delete old ones on a schedule:

```sql
-- daily at 03:00: delete workflows finished more than 7 days ago
select cron.schedule('resonate-gc', '0 3 * * *',
  $$select resonate.gc((extract(epoch from now())*1000 - 7*86400000)::bigint)$$);
```

Keep the horizon longer than any window in which you might re-send the same workflow id; ids are idempotent only while the row exists.

## Under the hood

resonate-pg is a faithful implementation of the Resonate protocol. Every protocol action is a stored procedure; `resonate_rpc` is the wire dispatcher in front of them, and `pg_cron` fires the timers. No additional servers, queues, or timers — Postgres, with pg_cron, is all three.

## How it's tested

`test/conformance.py` is a shim that puts an HTTP interface in front of `resonate_rpc`, so the external Resonate conformance harness can drive a resonate-pg database as if it were a Resonate server. It needs `psycopg`:

```bash
pip install 'psycopg[binary,pool]'
DATABASE_URL=postgres://... python test/conformance.py
```

The harness then runs against `RESONATE_URL=http://localhost:8001`.

There is no CI workflow on this repository, so nothing runs this on push or pull request.

## What's not there yet

- **No CI.** The conformance suite exists but nothing runs it on push or pull request.
- **No production reference deployments.** Nobody is running this in production yet.
- **Delivery depends on an optional extension.** Without `pg_net` or `pgsql_http`, there is no HTTP push path.

## Community

Questions, ideas, or want to help? Join the [Resonate Discord](https://resonatehq.io/discord), or open an issue or pull request — contributions welcome. resonate-pg is part of [Resonate](https://github.com/resonatehq/resonate).

## License

[Apache 2.0](./LICENSE).
