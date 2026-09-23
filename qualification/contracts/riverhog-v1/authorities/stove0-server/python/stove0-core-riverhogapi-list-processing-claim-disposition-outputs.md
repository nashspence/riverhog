# stove0_core.RiverhogApi.list_processing_claim_disposition_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-list-processing-c-0c4211e1bf:0c33e11fa3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-048af0937d"></a>
- <a id="s-31fb1cb97a"></a>`distribution`: `stove0-server`
- <a id="s-f2ed7122d0"></a>`module`: `stove0_core`
- <a id="s-ad1e59e454"></a>`name`: `list_processing_claim_disposition_outputs`
- <a id="s-6bbf455efa"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-28e2834a5b"></a>`unit`: `member`

### Declared structure

- <a id="s-e18577fb44"></a>`kind`: `"method"`
- <a id="s-6f74bc2311"></a>`signature`: `"\"(self, claim_id: 'str', *, identity_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionOutputPageDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-117fbc59c8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.list_processing_claim_disposition_outputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17d467d5ea27619a668bb9dff37ceff232747e049882759913fdb2686e6ced70 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, identity_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionOutputPageDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "list_processing_claim_disposition_outputs",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
