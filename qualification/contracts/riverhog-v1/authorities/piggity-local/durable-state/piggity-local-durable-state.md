# piggity-local durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:piggity-local:piggity-local-durable-state:570c36bae9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `piggity-local` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `format`: state-schema/sqlite

## Governing policies

- `compatibility/components/v1`

## Evidence

### Qualification

- `make release-check`
- `make database-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:piggity-local` — `state:piggity-local`

### Machine authority

- `/external_contract/durable_state/owners/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b61cb79a35d11a1b633bb803b5164736004af4cb0cee4b83e92530fa36f697e3 -->

```json
{
  "distribution": "piggity",
  "fixture_sha256s": [
    "f848ff7767d1fe3c70b8194c7b9334aaccf2f3c3ae26e3af99b48c86faf87a7f"
  ],
  "format": "state-schema/sqlite",
  "head": "v1_0001",
  "id": "piggity-local"
}
```
