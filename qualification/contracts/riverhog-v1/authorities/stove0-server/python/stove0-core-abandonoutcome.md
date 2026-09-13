# stove0_core.AbandonOutcome

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-abandonoutcome:62f3d5b23b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f87d5cf27c"></a>
| Field | Shape |
|---|---|
| <a id="s-c1ac5928a0"></a>`contract` | type="typing._LiteralGenericAlias"; additional keys=`kind` |
| <a id="s-853730936b"></a>`distribution` | "stove0-server" |
| <a id="s-7468926c3b"></a>`module` | "stove0_core" |
| <a id="s-58df0050a8"></a>`name` | "AbandonOutcome" |
| <a id="s-8e7835fc4f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-9a2a3866d9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.AbandonOutcome`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fd865761d3d7ecc2c8f47c89b67e91c25ab795c7625451298d7573e204a3c98 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "AbandonOutcome",
  "unit": "export"
}
```
