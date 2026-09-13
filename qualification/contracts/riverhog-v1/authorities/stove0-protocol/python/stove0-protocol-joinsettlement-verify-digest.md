# stove0_protocol.JoinSettlement.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinsettlement-verify-digest:c9b3758939 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d4b6c8ac6"></a>
| Field | Shape |
|---|---|
| <a id="s-03edbc6650"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-0d170e80ce"></a>`distribution` | "stove0-protocol" |
| <a id="s-1da778767a"></a>`module` | "stove0_protocol" |
| <a id="s-a854b71c38"></a>`name` | "verify_digest" |
| <a id="s-c94cc7fbaf"></a>`owner` | "stove0_protocol.JoinSettlement" |
| <a id="s-35d46052ad"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.JoinSettlement](stove0-protocol-joinsettlement.md)

## Governing policies

- <a id="pa-e9970bb8c5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinSettlement.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97d8664ebc5b9c7b861a7b79ea1be39137a4119813361a90b268fb3548a342b2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_digest",
  "owner": "stove0_protocol.JoinSettlement",
  "unit": "member"
}
```
