# stove0_core.RiverhogApi.list_processing_claim_outcomes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-list-processing-c-eb048df859:d7f8d6670d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b9ed50ba38"></a>
- <a id="s-e94f7d6941"></a>`distribution`: `stove0-server`
- <a id="s-f0b0ff279a"></a>`module`: `stove0_core`
- <a id="s-c22421a3e4"></a>`name`: `list_processing_claim_outcomes`
- <a id="s-9c2b229f95"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-0ce628392a"></a>`unit`: `member`

### Declared structure

- <a id="s-56bb48c64d"></a>`kind`: `"method"`
- <a id="s-0de0e8343e"></a>`signature`: `"\"(self, claim_id: 'str', *, identity_sha256: 'str', start_ordinal: 'int' = 0) -> 'ProcessingOutcomePageDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-7096e4c675"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.list_processing_claim_outcomes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 06bca69d7eedd47dcd0d68bc5e86f66041fd2f314eee227ea50372ce5ee21b7e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, identity_sha256: 'str', start_ordinal: 'int' = 0) -> 'ProcessingOutcomePageDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "list_processing_claim_outcomes",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
