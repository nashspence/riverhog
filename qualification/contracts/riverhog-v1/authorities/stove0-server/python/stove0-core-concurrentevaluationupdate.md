# stove0_core.ConcurrentEvaluationUpdate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-concurrentevaluationupdate:82d55f2ad4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0a0028423d"></a>
| Field | Shape |
|---|---|
| <a id="s-74108805e9"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-92bb3944fc"></a>`distribution` | "stove0-server" |
| <a id="s-5116322a2f"></a>`module` | "stove0_core" |
| <a id="s-a284f4066d"></a>`name` | "ConcurrentEvaluationUpdate" |
| <a id="s-ce56fde905"></a>`unit` | "export" |

## Governing policies

- <a id="pa-166f554846"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.ConcurrentEvaluationUpdate`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b20fc097ae41ec72d029d3e3fc79792144ec729a7e80be9b756d2ce0585f9089 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "unavailable"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ConcurrentEvaluationUpdate",
  "unit": "export"
}
```
