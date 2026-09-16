# riverhog-ftp-custody: completion-log

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-ftp-custody:riverhog-ftp-custody-completion-log:a1bbe7b9c4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-b65835b959"></a>



| Field | Value |
|---|---|
| <a id="s-7ffe049967"></a>`header` | `"riverhog-ftp-completion-log/v1 <canonical-uuid>"` |
| <a id="s-8eb9c6fee5"></a>`id` | `"completion-log"` |
| <a id="s-d12290b797"></a>`kind` | `"append-only-json-sequence"` |

### Record schema

<a id="s-343bf7cb65"></a>

- <a id="s-6a33552b85"></a>`type`: `"object"`
- <a id="s-6d380fc0be"></a>`additionalProperties`: `false`
- <a id="s-e40f88665e"></a>`required`: `["format","event_id","source_id","path","custody","bytes","device","inode"]`
- <a id="s-9671c143a0"></a>`title`: `"CompletionRecordState"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4dd43b05e3"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-c397d4f500"></a>`custody` | yes | type="string"; minLength=1 |  |
| <a id="s-c5c5a0d551"></a>`device` | yes | type="integer"; minimum=0 |  |
| <a id="s-cd3d8c2278"></a>`event_id` | yes | type="string"; minLength=1 |  |
| <a id="s-1123dbdc3b"></a>`format` | yes | type="string"; const="riverhog-ftp-completion-record/v1" |  |
| <a id="s-0ac116f9e2"></a>`inode` | yes | type="integer"; minimum=0 |  |
| <a id="s-57ea26efac"></a>`path` | yes | type="string"; minLength=1 |  |
| <a id="s-4e1045d512"></a>`source_id` | yes | type="string"; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-ftp-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-a0bfcb8259"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-ftp-custody](../../../evidence/sources.md#src-54f88a3a47) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/state_contract.py`

### Machine authority

- `/external_contract/durable_state/owners/6/structure/units/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b581ca5559b8368a097c4070cb2375a467fb192e17b43e51eceef0d8c080429a -->

```json
{
  "header": "riverhog-ftp-completion-log/v1 <canonical-uuid>",
  "id": "completion-log",
  "kind": "append-only-json-sequence",
  "record_schema": {
    "additionalProperties": false,
    "properties": {
      "bytes": {
        "minimum": 0,
        "title": "Bytes",
        "type": "integer"
      },
      "custody": {
        "minLength": 1,
        "title": "Custody",
        "type": "string"
      },
      "device": {
        "minimum": 0,
        "title": "Device",
        "type": "integer"
      },
      "event_id": {
        "minLength": 1,
        "title": "Event Id",
        "type": "string"
      },
      "format": {
        "const": "riverhog-ftp-completion-record/v1",
        "title": "Format",
        "type": "string"
      },
      "inode": {
        "minimum": 0,
        "title": "Inode",
        "type": "integer"
      },
      "path": {
        "minLength": 1,
        "title": "Path",
        "type": "string"
      },
      "source_id": {
        "minLength": 1,
        "title": "Source Id",
        "type": "string"
      }
    },
    "required": [
      "format",
      "event_id",
      "source_id",
      "path",
      "custody",
      "bytes",
      "device",
      "inode"
    ],
    "title": "CompletionRecordState",
    "type": "object"
  }
}
```

</details>
