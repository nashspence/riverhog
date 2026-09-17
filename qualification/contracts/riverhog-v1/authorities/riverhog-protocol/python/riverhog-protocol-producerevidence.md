# riverhog_protocol.ProducerEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-producerevidence:5fbec490d1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-acfba6eabb"></a>
- <a id="s-df04607a71"></a>`distribution`: `riverhog-protocol`
- <a id="s-115e4f9b88"></a>`module`: `riverhog_protocol`
- <a id="s-feae23b7a4"></a>`name`: `ProducerEvidence`
- <a id="s-d9b7d7007f"></a>`unit`: `export`

### Declared structure

- <a id="s-ec720104c5"></a>`kind`: `"class"`
- <a id="s-c1bc081d15"></a>`signature`: `"\"(producer_app: 'str', adapter_id: 'str', adapter_version: 'str', source_event_id: 'str', ingest_source: 'str', source_context: 'dict[str, JsonValue]') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-4c20e3f8dc"></a>`producer_app` | `'str'` | `required` |
| <a id="s-93505b23e3"></a>`adapter_id` | `'str'` | `required` |
| <a id="s-b6d10e7603"></a>`adapter_version` | `'str'` | `required` |
| <a id="s-b45a3e22c4"></a>`source_event_id` | `'str'` | `required` |
| <a id="s-a7bf2a5d68"></a>`ingest_source` | `'str'` | `required` |
| <a id="s-4f1aadff02"></a>`source_context` | `'dict[str, JsonValue]'` | `required` |

## Maintained corroboration

### Related interface records

- [as_dict](riverhog-protocol-producerevidence-as-dict.md)
- [from_mapping](riverhog-protocol-producerevidence-from-mapping.md)
- [sha256](riverhog-protocol-producerevidence-sha256.md)
- [to_json_bytes](riverhog-protocol-producerevidence-to-json-bytes.md)

## Governing policies

- <a id="pa-f810e4d074"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProducerEvidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0861fa2dee2cdaafc1767d45468c7dd73a10f141be00868992ba539ba22d875b -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "producer_app",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "adapter_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "adapter_version",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "source_event_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "ingest_source",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "source_context",
        "type": "'dict[str, JsonValue]'"
      }
    ],
    "kind": "class",
    "signature": "\"(producer_app: 'str', adapter_id: 'str', adapter_version: 'str', source_event_id: 'str', ingest_source: 'str', source_context: 'dict[str, JsonValue]') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProducerEvidence",
  "unit": "export"
}
```

</details>
