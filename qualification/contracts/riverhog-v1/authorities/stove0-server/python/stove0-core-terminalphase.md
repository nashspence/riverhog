# stove0_core.TerminalPhase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-terminalphase:1816595e51 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4add87bfad"></a>
| Field | Shape |
|---|---|
| <a id="s-f98aecc463"></a>`contract` | type="typing._LiteralGenericAlias"; additional keys=`kind` |
| <a id="s-3608756bdb"></a>`distribution` | "stove0-server" |
| <a id="s-cb16092efa"></a>`module` | "stove0_core" |
| <a id="s-572da82620"></a>`name` | "TerminalPhase" |
| <a id="s-8676f5ae22"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3832aacaad"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.TerminalPhase`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5dbd6b1e10587174afde4f0a9b034d95d934a8137bec9cee6d8d4340131e50c8 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "TerminalPhase",
  "unit": "export"
}
```
