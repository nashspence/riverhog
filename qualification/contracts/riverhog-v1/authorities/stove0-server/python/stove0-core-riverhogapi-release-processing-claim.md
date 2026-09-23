# stove0_core.RiverhogApi.release_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-release-processing-claim:cc6c4f1a5b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f92b03b642"></a>
- <a id="s-c6112104a7"></a>`distribution`: `stove0-server`
- <a id="s-8cf6a8dcd9"></a>`module`: `stove0_core`
- <a id="s-f72a8718cb"></a>`name`: `release_processing_claim`
- <a id="s-6830fe253d"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-3d9434503c"></a>`unit`: `member`

### Declared structure

- <a id="s-066faafa0d"></a>`kind`: `"method"`
- <a id="s-7510a296ed"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int') -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-715557bca2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.release_processing_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 604e705cc322403643419a0c827983a093bb003d55edcc2a2cec9721a6ffe8a4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "release_processing_claim",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
