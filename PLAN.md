# nyrkiov3 — PLAN

The application: analytics + change detection. Built on PureJson / ExtendedJsonSchema / JsonEE / ImplicitOpenApi / kuutar. Receives data from nyrkio-runner.

## Domain model

### Authz unit = the repo
Master plan: "test results and their change points belong to the repo where they originated. Each repo belongs to the namespace. Namespaces are within github (today; other platforms later)."

So the canonical identifier is `{platform}/{namespace}/{repo}`, e.g. `gh/henrikingo/git-perf`. Backend URLs encode this directly:
- `GET /api/v3/tests/gh/{namespace}/{repo}?branch=&test_name=&metric=&since=&until=`
- `GET /api/v3/pulls/gh/{namespace}/{repo}?...`

Pulls handled separately from main results — different lifecycle, different aggregation.

### What the master plan eliminates
- Separate endpoints for private/org/public — collapsed. Authorization is enforced by middleware looking at the repo's visibility, not by URL shape.
- Separate endpoints for push vs pull_request — split only at the `/tests` vs `/pulls` boundary, not within each.

Result: master plan said this kills 2^4 complexity. Verify by listing every existing v2 endpoint and mapping it to the v3 set; the count should drop dramatically. (Action item for the v3 implementation kickoff.)

## Change detection
- Apache Otava (latest) under the hood.
- Default `p ≈ 0.0001`, `threshold = 0`, `window_size` likely gone. Validate against the public dataset before defaulting.
- Per-graph and per-project thumbs-up / thumbs-down feedback drives `p` adjustment over time.
- Onboarding question: "false alarms or missed regressions?" → initial p choice.

## Datasets / bootstrap
- Pull `nyrkio.com/public` datasets into the v3 DocumentDB.
- Use as the prototyping corpus for kuutar and as regression-test fixtures for change detection.

## Site / docs separation
- Marketing/docs site is **separate** from the app.
- Markdown + images in a folder of the app repo (or sibling repo) — editable by non-programmers via GitHub web UI. No CMS for v1; revisit if non-technical editors complain.

## Testing
- Coverage tracked and reported to Nyrkiö itself (dogfooding) — but **not a hard target**. The reward for higher coverage is better auto-generated docs from ImplicitOpenApi (more endpoints exercised → tighter observed shapes), which is a natural pull instead of an enforced floor.
- Tests in same PR as code.
- `nyrkio-sandbox` separate registration with all 3rd parties (GitHub, Stripe-test, OneLogin/Okta) for continuous integration.

## DocumentDB choice (deferred from master PLAN)
Three real candidates after eliminating Mongo Inc:
1. **Microsoft DocumentDB** (Postgres-based, MIT, MongoDB-wire-compatible, 2024). Newest, leanest, Postgres operational maturity underneath.
2. **FerretDB** (Postgres-based, Apache-2.0, MongoDB-wire). More mature than #1 as of 2025.
3. AWS DocumentDB — managed, but not open source. Probably disqualified by the master plan's stated criteria.

Recommend FerretDB for v1 (operational maturity), with the PureJson DocumentDB adapter abstracted enough to swap.

<<< 1. is available as DBaaS (Yugabyte, maybe, on aws?) while FerretDB is withdrawing I think?

## Frontend framework
Two viable paths, decision deferred to implementation kickoff:

1. **htmx + Bootstrap** for the app shell (server-rendered HTML fragments, no build step, no SPA), with Svelte (or a vanilla three.js wrapper class) just around the kuutar canvas. Fewest moving parts; closest to the master plan's "maybe no framework at all" instinct. Good fit if JsonEE comfortably renders HTML alongside JSON.
2. **Svelte** for the whole frontend. More uniform, more familiar to frontend devs, larger client bundle.

Either way: ship a **proper client library** for the backend API (the v2 ad-hoc `fetch()` calls were called out as a smell). Generate it from the OpenAPI doc emitted by ImplicitOpenApi — closes the loop.

## Open questions
1. Multi-tenancy: **row-level in a single DocumentDB** with strong middleware enforcement of repo/namespace authz. Separate-DB-per-org is an **enterprise tier feature** — don't implement until a customer asks and pays. Same applies to dedicated infrastructure.
2. SSO — master plan notes "none of them actually used at the moment." Defer to v3.1 unless a customer asks.

<<< OneLogin is now used. But could be deferred a little bit. In fact, this is very much a GitHub-first project, so first version should be just GitHub auth.

## v2 → v3 cutover: keep them disconnected
**Resolved.** v2 stays running. v3 is a **separate platform** with its own GitHub App, its own Stripe account, its own database. Existing v2 users either keep using v2 or click the OAuth/app-install button on v3 to start fresh. No data migration, no Stripe customer ID continuity, no shared auth.

**Why this is right:**
- v2 has a small user base — migration engineering cost > migration value.
- A clean break means v3 architecture isn't constrained by v2 schemas or v2 Stripe IDs.
- Best case: a v2 user installs v3 and the historical-backfill feature ingests their last 3 months of GitHub workflow runs, giving them parity with their v2 history *without any migration tooling*. The wow-moment onboarding does the work.
- Worst case: a user runs both for a while during evaluation. Fine.

**Implication for nyrkio-runner:** new GitHub App registration, separate from v2. v2's app keeps running for its users.
