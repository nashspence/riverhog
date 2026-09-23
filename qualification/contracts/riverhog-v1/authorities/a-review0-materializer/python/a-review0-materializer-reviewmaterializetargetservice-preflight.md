# a_review0_materializer.ReviewMaterializeTargetService.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-materializer:a-review0-materializer-reviewmaterializet-780d02a8dd:d3eb542480 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0e7dcffe4a"></a>
- <a id="s-454fcbfdbb"></a>`distribution`: `a-review0-materializer`
- <a id="s-d09fbf5e66"></a>`module`: `a_review0_materializer`
- <a id="s-010a6dfbed"></a>`name`: `preflight`
- <a id="s-45fc86e9a4"></a>`owner`: `a_review0_materializer.ReviewMaterializeTargetService`
- <a id="s-5cc76ec7ee"></a>`unit`: `member`

### Declared structure

- <a id="s-e416ed7282"></a>`kind`: `"method"`
- <a id="s-f4bc26175d"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](a-review0-materializer-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-c557dcfd1b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-materializer:a_review0_materializer](../../../evidence/sources/authorities.md#src-fa69f32f84) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_materializer.ReviewMaterializeTargetService.preflight`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d0f94c0a72502fbce502cf589617041624515d2dfe5cb563322a3626d6641fa -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "a-review0-materializer",
  "module": "a_review0_materializer",
  "name": "preflight",
  "owner": "a_review0_materializer.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
