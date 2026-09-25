# OpenTimestamps collection witness

This supplied application independently witnesses immutable Riverhog collection identities.
It uses the same versioned `a-riverhog-collection-witness/v1` statement as the Minisign
witness, but owns a separate SQLite database, calendar work, proof history, and Bitcoin
verification path. It reads the official catalog sync API with an application key carrying
`catalog:read`. It never gates collection finalization.

## Run

Set `RIVERHOG_BASE_URL` and `RIVERHOG_TOKEN` for a generic Riverhog instance. Choose exact
HTTPS calendar base URLs explicitly; there is no implicit calendar or wildcard allowlist.
The following `.example` host is illustrative and is not a deployed calendar:

```sh
a-riverhog-opentimestamps-witness --state ./witness.sqlite3 state upgrade
a-riverhog-opentimestamps-witness --state ./witness.sqlite3 ingest \
  --calendar https://calendar.example
a-riverhog-opentimestamps-witness --state ./witness.sqlite3 mature \
  --calendar https://calendar.example
```

`ingest` commits one bounded catalog page, its cursor, a random blinded nonce, and pending
submission work in one transaction before any calendar request. `mature` performs at most one
bounded exchange and retains each changed proof revision. Schedule both independently, or
run `run --calendar ... --poll-seconds 60`. Calendar failures retain evidence and use bounded
retry intervals. A proof may remain pending for a long time. `evidence <statement-sha256>`
exports the exact statement, current proof, durable job, and proof revision history as JSON;
proof and job bytes are base64 encoded.

On a same-view departure, the app records `collection_deleted` or `visibility_lost` and
retains prior evidence. A grant/view change stops catalog sync. After inspecting it, run
`rebaseline` explicitly; old observations and proofs remain. No per-item departure is
fabricated across views. If a calendar allowlist changes, use `reschedule <statement-sha256>`
to reconsider a paused job with the new list.

`verify <statement-sha256> --bitcoin-rpc-url ... --bitcoin-cookie ...` recomputes the proof
against an operator-selected, synchronized, fully validating Bitcoin Core mainnet node. The
RPC port permits only chain reads and needs no wallet. A valid Bitcoin attestation proves
existence by the observed block-header time, not an exact creation time. It is not inferred
from a pending calendar marker. `--minimum-confirmations` is optional application policy:
it does not alter cryptographic validity. Verification is recomputed each time so a chain
reorganization cannot leave a sticky success result. Use HTTPS for remote RPC or HTTP only
on the loopback host.

The app and image are published independently from Riverhog core. The image owns `/state`
for its SQLite file; mount a durable volume there and supply the Riverhog token and Bitcoin
RPC cookie through deployment-owned secrets. No real deployment identities belong in this
repository.
