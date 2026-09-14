# stove0_core.RiverhogApi.get_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-get-processing-claim:f5f442bf7d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e8a532bd0b"></a>
- <a id="s-dacb572444"></a>`distribution`: `stove0-server`
- <a id="s-74878b96ac"></a>`module`: `stove0_core`
- <a id="s-3a7ac233e7"></a>`name`: `get_processing_claim`
- <a id="s-d91d9a3ee8"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-453cc013a6"></a>`unit`: `member`

### Declared structure

- <a id="s-7cfe2ec248"></a>`kind`: `"method"`
- <a id="s-799059df49"></a>`signature`: `"\"(self, claim_id: 'str') -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-4ed7d438cf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.get_processing_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 719421c5fbd971adcf1121c8a716a5a6dddfb217013f33c795e25a1320af8f91 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "get_processing_claim",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```
