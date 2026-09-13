# stove0_protocol.BranchEffectSettlement.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-brancheffectsettlement-seal:582399e751 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-22fe5c6922"></a>
| Field | Shape |
|---|---|
| <a id="s-8c06202d53"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4cb73e07dd"></a>`distribution` | "stove0-protocol" |
| <a id="s-acadc16914"></a>`module` | "stove0_protocol" |
| <a id="s-5a397ad8bf"></a>`name` | "seal" |
| <a id="s-27c751fb9c"></a>`owner` | "stove0_protocol.BranchEffectSettlement" |
| <a id="s-8326af1c53"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchEffectSettlement](stove0-protocol-brancheffectsettlement.md)

## Governing policies

- <a id="pa-d24f3ed9e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchEffectSettlement.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74e7125c7d8e52edf4b98fccab786ee3eee4cbaf5689e9db50e98321dba840f8 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, branch: 'BranchPlan', effect_receipt_sha256: 'str') -> 'BranchEffectSettlement'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.BranchEffectSettlement",
  "unit": "member"
}
```
