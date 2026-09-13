# stove0_core.Stove0RiverhogClient.retire_input

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-retire-input:41e2016de0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-281f9845c7"></a>
| Field | Shape |
|---|---|
| <a id="s-1861ab8585"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-23dc60ad81"></a>`distribution` | "stove0-server" |
| <a id="s-527336ef88"></a>`module` | "stove0_core" |
| <a id="s-900e670609"></a>`name` | "retire_input" |
| <a id="s-50500cdf56"></a>`owner` | "stove0_core.Stove0RiverhogClient" |
| <a id="s-ae9a7d150f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-4e81814322"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.retire_input`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 53830f502d99e26ca010fbe793fe174b9002d58bf8effa136d1a15efa99915f3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', collection_id: 'int') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retire_input",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
