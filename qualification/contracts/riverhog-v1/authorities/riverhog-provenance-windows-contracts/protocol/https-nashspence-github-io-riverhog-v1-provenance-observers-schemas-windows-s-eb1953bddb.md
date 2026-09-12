# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-eb1953bddb:459d9fbc6e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-78a2bea4a4b2) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6e9034e67827"></a>
- <a id="s-8c3c39f31fd6"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json
- <a id="s-7b4c20b04837"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7a0231ddf733"></a>`control` | yes | type="integer"; minimum=0 |  |
| <a id="s-bce011dd2b1e"></a>`group_sid` | yes | type="string"; pattern="^(?:\|S-[0-9-]+)$" |  |
| <a id="s-5a605b631484"></a>`owner_sid` | yes | type="string"; pattern="^(?:\|S-[0-9-]+)$" |  |
| <a id="s-f6a3600101d9"></a>`sacl_included` | yes | type="boolean" |  |
| <a id="s-a2cb87b1635d"></a>`security_information` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-e701d5cba2de"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json](../../../evidence/sources.md#src-1bd0c5feb8ca) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-security-descriptor.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-security-descriptor.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60dd20fce868257efd0bb6c6f4e9c85e392b00a5a9614541a4762da252705893 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "control": {
      "minimum": 0,
      "type": "integer"
    },
    "group_sid": {
      "pattern": "^(?:|S-[0-9-]+)$",
      "type": "string"
    },
    "owner_sid": {
      "pattern": "^(?:|S-[0-9-]+)$",
      "type": "string"
    },
    "sacl_included": {
      "type": "boolean"
    },
    "security_information": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "security_information",
    "control",
    "owner_sid",
    "group_sid",
    "sacl_included"
  ],
  "type": "object"
}
```
