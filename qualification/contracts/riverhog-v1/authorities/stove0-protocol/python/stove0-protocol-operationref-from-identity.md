# stove0_protocol.OperationRef.from_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-operationref-from-identity:8bffaff1e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc26c0d50e"></a>
| Field | Shape |
|---|---|
| <a id="s-96909b6f58"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-6be04e32fe"></a>`distribution` | "stove0-protocol" |
| <a id="s-a6f247f303"></a>`module` | "stove0_protocol" |
| <a id="s-25bfc4cf45"></a>`name` | "from_identity" |
| <a id="s-55bde13610"></a>`owner` | "stove0_protocol.OperationRef" |
| <a id="s-c7169f0ed3"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.OperationRef](stove0-protocol-operationref.md)

## Governing policies

- <a id="pa-2171364a30"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.OperationRef.from_identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88980458cb381543c3b8407b0ba99ad39a977f1b90ec5083d8e936bb4ef3ea53 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'OperationIdentity') -> 'OperationRef'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "from_identity",
  "owner": "stove0_protocol.OperationRef",
  "unit": "member"
}
```
