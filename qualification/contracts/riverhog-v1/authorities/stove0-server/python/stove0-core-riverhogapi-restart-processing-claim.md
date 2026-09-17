# stove0_core.RiverhogApi.restart_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-restart-processing-claim:e83cfb31d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9dc9b816a0"></a>
- <a id="s-bd0516675b"></a>`distribution`: `stove0-server`
- <a id="s-54834addc8"></a>`module`: `stove0_core`
- <a id="s-e278918bab"></a>`name`: `restart_processing_claim`
- <a id="s-8790376a2d"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-f5e3fb3970"></a>`unit`: `member`

### Declared structure

- <a id="s-8e8d0355d0"></a>`kind`: `"method"`
- <a id="s-e122418ca4"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int', lease_seconds: 'int' = 1800) -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-85a4adf86a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.restart_processing_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77d87feb78e83290a41a5b6abdc3d75f0ca56ee7344da03f94feb020aa66831e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int', lease_seconds: 'int' = 1800) -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "restart_processing_claim",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
