# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-linux-contracts:https-nashspence-github-io-riverhog-v1-pr-0cf0b9725d:3b08917ffb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-linux-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `raw` | yes | type="integer"; minimum=0 |  |
| `set_names` | yes | type="array"; items=(type="string"); additional keys=`uniqueItems` |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json` — `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/schemas/linux-fs-flags.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1linux-fs-flags.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: db4e0172d68ed4679f01a318962ea89f52c0ae405d53335fc21458b7b9cf74ec -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "raw": {
      "minimum": 0,
      "type": "integer"
    },
    "set_names": {
      "items": {
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    }
  },
  "required": [
    "raw",
    "set_names"
  ],
  "type": "object"
}
```
