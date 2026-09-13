# stove0_target_protocol.TargetProtocolModel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetprotocolmodel:09578bd330 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c609296245"></a>
| Field | Shape |
|---|---|
| <a id="s-380c5f7684"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2923974e6c"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-bf3b93adaa"></a>`module` | "stove0_target_protocol" |
| <a id="s-0d704363d3"></a>`name` | "TargetProtocolModel" |
| <a id="s-ac4b934a57"></a>`unit` | "export" |

## Governing policies

- <a id="pa-e19d717ad8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProtocolModel`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8cf1d9f0e8199a035a3f88bfa475c3c124a50a777574cf6a99c23e97af432207 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c93552e6289487dd5fa8baeb3416b24ed8657c354167fb0ed7a8de1e1a8ccb90",
    "signature": "'() -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetProtocolModel",
  "unit": "export"
}
```
