# stove0_core.RiverhogApi.settle_processing_claim_outcomes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-settle-processing-d67269c6eb:950a2ea56e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-46770a3595"></a>
- <a id="s-4470336d03"></a>`distribution`: `stove0-server`
- <a id="s-8ce92152f1"></a>`module`: `stove0_core`
- <a id="s-9db9127e4f"></a>`name`: `settle_processing_claim_outcomes`
- <a id="s-168ea27291"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-f856dddf4f"></a>`unit`: `member`

### Declared structure

- <a id="s-9934f47acd"></a>`kind`: `"method"`
- <a id="s-b8b81de746"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-1443d0a65f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.settle_processing_claim_outcomes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 162ef25be23b9c339078afe7245d402b12a337274ab284fe60f6175223959b35 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "settle_processing_claim_outcomes",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
