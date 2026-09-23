# a_review0_materializer.ReviewMaterializeTargetService.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-materializer:a-review0-materializer-reviewmaterializet-e78ced38d9:cffa6324e8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8565a1ed56"></a>
- <a id="s-8f766c0dc5"></a>`distribution`: `a-review0-materializer`
- <a id="s-08d98677ae"></a>`module`: `a_review0_materializer`
- <a id="s-57e679d6d1"></a>`name`: `close`
- <a id="s-e1fdd585c6"></a>`owner`: `a_review0_materializer.ReviewMaterializeTargetService`
- <a id="s-12a24b780c"></a>`unit`: `member`

### Declared structure

- <a id="s-20afa27d0d"></a>`kind`: `"method"`
- <a id="s-059401dbb3"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](a-review0-materializer-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-42009d07cb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-materializer:a_review0_materializer](../../../evidence/sources/authorities.md#src-fa69f32f84) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_materializer.ReviewMaterializeTargetService.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a4ae5eb9c71ea097d01bd1ae51b580f8af51a4a3fde472fdb7784377a7440fa -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "a-review0-materializer",
  "module": "a_review0_materializer",
  "name": "close",
  "owner": "a_review0_materializer.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
