# a_review0_materializer.ReviewMaterializeTargetService.readiness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-materializer:a-review0-materializer-reviewmaterializet-44eee930b3:8fc3567c99 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e016ec18b"></a>
- <a id="s-b47f287d18"></a>`distribution`: `a-review0-materializer`
- <a id="s-d2d9bd1852"></a>`module`: `a_review0_materializer`
- <a id="s-ca35547b62"></a>`name`: `readiness`
- <a id="s-1eacfa36d4"></a>`owner`: `a_review0_materializer.ReviewMaterializeTargetService`
- <a id="s-3b5a510aba"></a>`unit`: `member`

### Declared structure

- <a id="s-b8e6e04b13"></a>`kind`: `"method"`
- <a id="s-6b1a29a0d9"></a>`signature`: `"\"(self) -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](a-review0-materializer-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-ca4900a921"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-materializer:a_review0_materializer](../../../evidence/sources/authorities.md#src-fa69f32f84) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_materializer.ReviewMaterializeTargetService.readiness`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad004c275925f71f7a594c4972fefe78095e94ac9a1d9e2348701aac33ec884b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, str]'\""
  },
  "distribution": "a-review0-materializer",
  "module": "a_review0_materializer",
  "name": "readiness",
  "owner": "a_review0_materializer.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
