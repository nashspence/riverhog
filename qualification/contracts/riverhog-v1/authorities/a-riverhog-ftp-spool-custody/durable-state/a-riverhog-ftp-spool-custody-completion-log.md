# a-riverhog-ftp-spool-custody: completion-log

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-ftp-spool-custody:a-riverhog-ftp-spool-custody-completion-log:1386ead835 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-73f151460c"></a>



| Field | Value |
|---|---|
| <a id="s-0e624e796c"></a>`header` | `"riverhog-ftp-completion-log/v1 <canonical-uuid>"` |
| <a id="s-007d90bfd3"></a>`id` | `"completion-log"` |
| <a id="s-12f14f89f3"></a>`kind` | `"append-only-json-sequence"` |

### Record schema

<a id="s-88e143bc7d"></a>

- <a id="s-7711a4e7a6"></a>`type`: `"object"`
- <a id="s-5faef76ab4"></a>`additionalProperties`: `false`
- <a id="s-18906449dc"></a>`required`: `["format","event_id","source_id","path","custody","bytes","device","inode"]`
- <a id="s-628cfa2262"></a>`title`: `"CompletionRecordState"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b95009d127"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-f42fcf5b02"></a>`custody` | yes | type="string"; minLength=1; title="Custody" |  |
| <a id="s-9205b0a84a"></a>`device` | yes | type="integer"; minimum=0; title="Device" |  |
| <a id="s-c92de62cb0"></a>`event_id` | yes | type="string"; minLength=1; title="Event Id" |  |
| <a id="s-529746f93b"></a>`format` | yes | type="string"; const="riverhog-ftp-completion-record/v1"; title="Format" |  |
| <a id="s-5af51d3229"></a>`inode` | yes | type="integer"; minimum=0; title="Inode" |  |
| <a id="s-38312d3990"></a>`path` | yes | type="string"; minLength=1; title="Path" |  |
| <a id="s-8074627bd1"></a>`source_id` | yes | type="string"; minLength=1; title="Source Id" |  |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-ftp-spool-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-b37afd2857"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-ftp-spool-custody](../../../evidence/sources/authorities.md#src-97232522a5) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/state\_contract.py::ftp\_custody\_state\_contract](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/state_contract.py)

### Machine authority

- `/external_contract/durable_state/owners/8/structure/units/1`

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
