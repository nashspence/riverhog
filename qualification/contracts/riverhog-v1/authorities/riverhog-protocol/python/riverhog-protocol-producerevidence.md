# riverhog_protocol.ProducerEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-producerevidence:5fbec490d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-acfba6eabb"></a>
| Field | Shape |
|---|---|
| <a id="s-41ad8cd43c"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-df04607a71"></a>`distribution` | "riverhog-protocol" |
| <a id="s-115e4f9b88"></a>`module` | "riverhog_protocol" |
| <a id="s-feae23b7a4"></a>`name` | "ProducerEvidence" |
| <a id="s-d9b7d7007f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ProducerEvidence.as_dict](riverhog-protocol-producerevidence-as-dict.md)
- [riverhog_protocol.ProducerEvidence.from_mapping](riverhog-protocol-producerevidence-from-mapping.md)
- [riverhog_protocol.ProducerEvidence.sha256](riverhog-protocol-producerevidence-sha256.md)
- [riverhog_protocol.ProducerEvidence.to_json_bytes](riverhog-protocol-producerevidence-to-json-bytes.md)

## Governing policies

- <a id="pa-f810e4d074"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProducerEvidence`

### Exact owned JSON

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
