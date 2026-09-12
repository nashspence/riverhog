# stove0-control durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-durable-state:d6236ad32e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-control` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `format`: state-schema/postgresql

## Governing policies

- `compatibility/components/v1`

## Evidence

### Qualification

- `make release-check`
- `make database-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:stove0-control` — `state:stove0-control`

### Machine authority

- `/external_contract/durable_state/owners/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6aef2ad022b96817b83eb895c8f2d6d69a5a7972b6f984cf7d2eb18de39505d1 -->

```json
{
  "distribution": "stove0-server",
  "fixture_sha256s": [
    "6fec3eecfca50325cdc0823fd9d2b301fbbbd3cf272abd5fe58ac3fb8dca7a29"
  ],
  "format": "state-schema/postgresql",
  "head": "v1_0001",
  "id": "stove0-control"
}
```
