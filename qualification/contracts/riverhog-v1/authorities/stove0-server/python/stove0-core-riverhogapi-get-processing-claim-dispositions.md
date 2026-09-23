# stove0_core.RiverhogApi.get_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-get-processing-cl-14efe7acfd:1c15152453 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-93b871d847"></a>
- <a id="s-4e0113ea00"></a>`distribution`: `stove0-server`
- <a id="s-d5956dae65"></a>`module`: `stove0_core`
- <a id="s-78887db0d0"></a>`name`: `get_processing_claim_dispositions`
- <a id="s-055f308f22"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-5339dace0d"></a>`unit`: `member`

### Declared structure

- <a id="s-5ca81b6987"></a>`kind`: `"method"`
- <a id="s-081b999aea"></a>`signature`: `"\"(self, claim_id: 'str') -> 'ArtifactDispositionSetDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-13bfefffb6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.get_processing_claim_dispositions`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a519799db136dd7c180ffc5bfa22bb21dd320397f167f8a82919c77c1be557c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str') -> 'ArtifactDispositionSetDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "get_processing_claim_dispositions",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
