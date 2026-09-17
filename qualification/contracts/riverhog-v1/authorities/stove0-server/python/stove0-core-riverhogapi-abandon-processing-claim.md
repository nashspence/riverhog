# stove0_core.RiverhogApi.abandon_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-abandon-processing-claim:5025cdcaf3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-73a14f24a0"></a>
- <a id="s-2e15f160da"></a>`distribution`: `stove0-server`
- <a id="s-ce58934280"></a>`module`: `stove0_core`
- <a id="s-d7b6aa40b3"></a>`name`: `abandon_processing_claim`
- <a id="s-461a3a3ad9"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-e1228e929a"></a>`unit`: `member`

### Declared structure

- <a id="s-39c4fa82f5"></a>`kind`: `"method"`
- <a id="s-7fde3eb1d7"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int', reason: 'str') -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-5b932c6730"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.abandon_processing_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 943b16a4b3f3996d3170b328c571bcb669a3c1d92597971794ab7e74d41e97f6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int', reason: 'str') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "abandon_processing_claim",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
