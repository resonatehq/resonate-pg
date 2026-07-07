# Countdown on Supabase — zero to a durable workflow

This is the complete, step-by-step setup of resonate-pg on Supabase: install the
server, deploy an Edge Function, start a workflow, watch it run. About five
minutes.

The workflow is a countdown: it logs `n`, waits a second, and repeats until
liftoff. The waiting is the point — while it waits, **nothing is running
anywhere**. Change the sleep to a day and you have a daily step; the function
invocations stay milliseconds long either way. Edge Functions get killed after a
few minutes; this workflow could run for a year.

```ts
resonate.register(
  "countdown",
  function* countdown(ctx: Context, n: number): Generator {
    for (let i = n; i > 0; i--) {
      yield* ctx.run(() => console.log(`countdown: ${i}`));
      yield* ctx.sleep(1000);
    }
    yield* ctx.run(() => console.log("liftoff 🚀"));
  },
);
```

## What you need

- A Supabase project — hosted at [supabase.com](https://supabase.com), or local
  with `supabase start`.
- The [Supabase CLI](https://supabase.com/docs/guides/local-development) to
  deploy the function.

## Step 1 — Install the server

Open the **SQL Editor** in your Supabase dashboard, paste the entire contents of
[`resonate.sql`](../../resonate.sql), and run it. (Or from your machine:
`psql "$YOUR_DB_CONNECTION_STRING" -f resonate.sql`.)

That's the whole server: one `resonate` schema, a `resonate_worker` role, and
one pg_cron job — nothing else touched. Verify it's alive — this asks the
server for a promise that doesn't exist yet:

```sql
select resonate.resonate_rpc('{"kind":"promise.get","head":{},"data":{"id":"hello"}}');
```

You should get back a response with `"status": 404` in its `head` — the server
is answering.

Then check the timer is ticking:

```sql
select jobname, schedule from cron.job where jobname = 'resonate_process_timeouts';
```

One row (`5 seconds`) means timers drive themselves. On pg_cron older than 1.5
the schedule reads `* * * * *` instead — timers tick once a minute rather than
every five seconds, and everything still works. **No row?** Enable the
**pg_cron** extension (Dashboard → Database → Extensions) and run `resonate.sql`
again — re-running is always safe.

One more check: **pg_net** is what lets the database call your function back.
Make sure it's enabled too (same Extensions page):

```sql
select count(*) from pg_extension where extname = 'pg_net';
```

## Step 2 — Deploy the function

In your Supabase project directory:

```bash
supabase functions new countdown
```

Replace the generated `supabase/functions/countdown/index.ts` with
[`index.ts`](index.ts) from this folder, then deploy:

```bash
supabase functions deploy countdown --no-verify-jwt
```

There is nothing to configure: the function reaches the server over the
`SUPABASE_DB_URL` environment variable that Supabase injects into every Edge
Function.

> **Local dev:** with `supabase functions serve`, also pass an env file with
> `FUNCTION_URL=http://kong:8000/functions/v1/countdown` so steps route back to
> the function through the local gateway.

## Step 3 — Start a countdown

A workflow starts when you create a **durable promise** aimed at your function.
In the SQL Editor (swap `<project-ref>` for yours — it's in your dashboard URL):

```sql
select resonate.resonate_rpc(jsonb_build_object(
  'kind', 'promise.create', 'head', '{}'::jsonb,
  'data', jsonb_build_object(
    'id',        'countdown-demo',                                    -- any unique id
    'timeoutAt', (extract(epoch from now()) * 1000)::bigint + 600000, -- give up after 10 min
    'param', jsonb_build_object(
      'headers', '{}'::jsonb,
      -- the invocation, base64-encoded: countdown(3)
      'data', replace(encode(convert_to(
                '{"func":"countdown","args":[3],"version":1}', 'utf8'), 'base64'), E'\n', '')),
    'tags', jsonb_build_object(
      'resonate:target', 'https://<project-ref>.functions.supabase.co/countdown',
      'resonate:scope',  'global',
      'resonate:origin', 'countdown-demo',
      'resonate:branch', 'countdown-demo',
      'resonate:parent', 'countdown-demo'))));
```

The database pushes the first call to your function immediately (pg_net makes
the HTTP call). From here the run drives itself: sleep, timer, resume, repeat.

## Step 4 — Watch it run

**The logs:** Dashboard → Edge Functions → countdown → Logs. You'll see
`countdown: 3`, `countdown: 2`, `countdown: 1`, `liftoff 🚀` — each from a
**separate invocation**, seconds apart, with nothing running in between.

**The state:** every step is a row you can query.

```sql
select id, state from resonate.promises where id like 'countdown-demo%' order by id;
```

The tree grows one child per step (`.0`, `.1`, `.2`, …— a log step, a sleep, a
log step, …), each flipping to `resolved` as the workflow advances. When the
root resolves, get the result:

```sql
select convert_from(decode(value_data, 'base64'), 'utf8')
from resonate.promises where id = 'countdown-demo';
```

`"liftoff"` 🚀

Run it again with `'args':[10000]` — and a `timeoutAt` to match, say
`+ 4 * 3600000` (four hours) instead of `+ 600000` — and it will tick for close
to three hours: thousands of invocations, each a few milliseconds of compute,
none of them anywhere near a timeout.

## What just happened

- `ctx.run(...)` executed a step and saved its result as a row. Saved steps are
  never re-executed, which is why each number logs exactly once even though the
  function was invoked many times.
- `ctx.sleep(1000)` wrote a deadline and suspended the workflow. No process
  waited: pg_cron fired the timer, pg_net called the function back, and a fresh
  invocation skipped the saved steps and ran the next one.
- Every step is saved, so the workflow also survives crashes, redeploys, and
  restarts. It continues from the last saved step, never from zero.

That's durable execution, on nothing but your Supabase Postgres. The
[main README](../../README.md) has the full picture: how it works, operations,
and what's under the hood.

## Cleanup

```sql
select resonate.gc((extract(epoch from now()) * 1000)::bigint);  -- delete finished workflows
```
