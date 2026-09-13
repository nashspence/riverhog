# stove0_core.RiverhogApi.begin_processing_claim_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-begin-processing-0f5d7224f2:cd1871e0c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c02be6d1bc"></a>
| Field | Shape |
|---|---|
| <a id="s-e99ba13bb4"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-32de6655df"></a>`distribution` | "stove0-server" |
| <a id="s-0d123fe3c1"></a>`module` | "stove0_core" |
| <a id="s-c23655ed45"></a>`name` | "begin_processing_claim_retirement" |
| <a id="s-19bd7ae225"></a>`owner` | "stove0_core.RiverhogApi" |
| <a id="s-d4f2acd425"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-9fb623a3cb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.begin_processing_claim_retirement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b3d927b7e8119f3d11d0bb8a814d9bed3039bc5e1010ce07d4232b20cec525d5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "begin_processing_claim_retirement",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```
