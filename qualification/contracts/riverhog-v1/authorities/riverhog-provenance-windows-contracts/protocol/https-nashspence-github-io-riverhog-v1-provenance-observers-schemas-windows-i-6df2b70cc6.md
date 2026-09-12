# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-6df2b70cc6:a12e386383 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `checksum_algorithm` | yes | type="integer"; minimum=0 |  |
| `checksum_chunk_size` | yes | type="integer"; minimum=0 |  |
| `cluster_size` | yes | type="integer"; minimum=0 |  |
| `flags` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-integrity-info.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-integrity-info.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 986c12768cb14d0aceae4c926c74f7230aac4e51cd9cbb0c04e6453b35a2aaad -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "checksum_algorithm": {
      "minimum": 0,
      "type": "integer"
    },
    "checksum_chunk_size": {
      "minimum": 0,
      "type": "integer"
    },
    "cluster_size": {
      "minimum": 0,
      "type": "integer"
    },
    "flags": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "checksum_algorithm",
    "flags",
    "checksum_chunk_size",
    "cluster_size"
  ],
  "type": "object"
}
```
