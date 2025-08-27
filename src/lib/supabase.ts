import { createClient } from "@supabase/supabase-js";

// Cliente público (seguro para frontend)
export const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
);
