# a_review0_materializer.ReviewMaterializeTargetService.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-materializer:a-review0-materializer-reviewmaterializet-e6b1964459:b6f80aed16 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f4839a1a4"></a>
- <a id="s-f28c4612dc"></a>`distribution`: `a-review0-materializer`
- <a id="s-38a25f3488"></a>`module`: `a_review0_materializer`
- <a id="s-5d7e639114"></a>`name`: `contract`
- <a id="s-df91bdbeb5"></a>`owner`: `a_review0_materializer.ReviewMaterializeTargetService`
- <a id="s-38410af6fa"></a>`unit`: `member`

### Declared structure

- <a id="s-325ae6daa5"></a>`kind`: `"method"`
- <a id="s-9cdbde4454"></a>`signature`: `"\"(self) -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](a-review0-materializer-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-1ba7842857"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-materializer:a_review0_materializer](../../../evidence/sources/authorities.md#src-fa69f32f84) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_materializer.ReviewMaterializeTargetService.contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9281ed345de64ee8663911c713d86e661057fc072f628c3752a431e9a81c3732 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "a-review0-materializer",
  "module": "a_review0_materializer",
  "name": "contract",
  "owner": "a_review0_materializer.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
