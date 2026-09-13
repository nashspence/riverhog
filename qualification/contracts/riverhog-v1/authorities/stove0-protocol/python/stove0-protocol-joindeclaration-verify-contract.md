# stove0_protocol.JoinDeclaration.verify_contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joindeclaration-verify-contract:4cf37917ee -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dac38cb8a6"></a>
| Field | Shape |
|---|---|
| <a id="s-a5b1dca762"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f19d1c6f10"></a>`distribution` | "stove0-protocol" |
| <a id="s-84949f3496"></a>`module` | "stove0_protocol" |
| <a id="s-b5239a2fd1"></a>`name` | "verify_contract" |
| <a id="s-27e557d654"></a>`owner` | "stove0_protocol.JoinDeclaration" |
| <a id="s-f8257d42e9"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.JoinDeclaration](stove0-protocol-joindeclaration.md)

## Governing policies

- <a id="pa-2c7285bddf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinDeclaration.verify_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a589861ed59addeb25981f5864f319bfe91fc17706e901aa5d56e44031386d57 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_contract",
  "owner": "stove0_protocol.JoinDeclaration",
  "unit": "member"
}
```
