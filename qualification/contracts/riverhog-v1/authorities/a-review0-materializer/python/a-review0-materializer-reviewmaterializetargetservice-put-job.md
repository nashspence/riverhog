# a_review0_materializer.ReviewMaterializeTargetService.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-materializer:a-review0-materializer-reviewmaterializet-e2185926c0:1d9fb9ee48 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-996a7edb14"></a>
- <a id="s-f71280e2b7"></a>`distribution`: `a-review0-materializer`
- <a id="s-d2ef82e67d"></a>`module`: `a_review0_materializer`
- <a id="s-d425ad5078"></a>`name`: `put_job`
- <a id="s-0668393bb1"></a>`owner`: `a_review0_materializer.ReviewMaterializeTargetService`
- <a id="s-7a81780193"></a>`unit`: `member`

### Declared structure

- <a id="s-e6c5695f20"></a>`kind`: `"method"`
- <a id="s-9d081c7da7"></a>`signature`: `"\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](a-review0-materializer-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-4098e7084a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-materializer:a_review0_materializer](../../../evidence/sources/authorities.md#src-fa69f32f84) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_materializer.ReviewMaterializeTargetService.put_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1b1cf82d11f0befd3fa79bf755436f6da83de64403231fb55d026dbcff767f9b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""
  },
  "distribution": "a-review0-materializer",
  "module": "a_review0_materializer",
  "name": "put_job",
  "owner": "a_review0_materializer.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
