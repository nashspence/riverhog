# stove0_core.RiverhogApi.record_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-record-processing-9b7359fc2c:b7796d006f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-048ef09666"></a>
- <a id="s-b7ba4e8b86"></a>`distribution`: `stove0-server`
- <a id="s-d3dab9a7f1"></a>`module`: `stove0_core`
- <a id="s-a28aca684b"></a>`name`: `record_processing_claim_dispositions`
- <a id="s-608cd56833"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-2c91b6666c"></a>`unit`: `member`

### Declared structure

- <a id="s-ed18ccae39"></a>`kind`: `"method"`
- <a id="s-ab8090a93c"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int', dispositions: 'Sequence[Mapping[str, Any]]') -> 'ArtifactDispositionSetDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-ad62db1475"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.record_processing_claim_dispositions`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5e5707aff35bb6b9d29615d14cf239ed855a326c045b1675ecdb1d595189902d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int', dispositions: 'Sequence[Mapping[str, Any]]') -> 'ArtifactDispositionSetDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_processing_claim_dispositions",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
