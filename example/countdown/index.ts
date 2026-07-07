// An Edge Function that takes time.
import { type Context, Resonate } from "jsr:@resonatehq/supabase@^0.4.0";

const resonate = new Resonate();

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

resonate.httpHandler();
