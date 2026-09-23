# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:a-riverhog-windows-provenance-contract-lib:https-nashspence-github-io-riverhog-v1-pr-eb1953bddb:573c09dbfa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-windows-provenance-contract-lib](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-6e9034e678"></a>

- <a id="s-7b4c20b048"></a>`type`: `"object"`
- <a id="s-8c3c39f31f"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json"`
- <a id="s-098978f50c"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-9e41032a88"></a>`additionalProperties`: `false`
- <a id="s-d7ad16218c"></a>`required`: `["security_information","control","owner_sid","group_sid","sacl_included"]`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7a0231ddf7"></a>`control` | yes | type="integer"; minimum=0 |  |
| <a id="s-bce011dd2b"></a>`group_sid` | yes | type="string"; pattern="^(?:\|S-[0-9-]+)$" |  |
| <a id="s-5a605b6314"></a>`owner_sid` | yes | type="string"; pattern="^(?:\|S-[0-9-]+)$" |  |
| <a id="s-f6a3600101"></a>`sacl_included` | yes | type="boolean" |  |
| <a id="s-a2cb87b163"></a>`security_information` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-123eec9239"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json](../../../evidence/sources/authorities.md#src-1bd0c5feb8) — [some-implementations/riverhog/provenance/contracts/windows/src/a\_riverhog\_windows\_provenance\_contract\_lib/schemas/windows-security-descriptor.schema.json](../../../../../../some-implementations/riverhog/provenance/contracts/windows/src/a_riverhog_windows_provenance_contract_lib/schemas/windows-security-descriptor.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-security-descriptor.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
