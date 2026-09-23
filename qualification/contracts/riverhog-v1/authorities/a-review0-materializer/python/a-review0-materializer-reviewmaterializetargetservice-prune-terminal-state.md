# a_review0_materializer.ReviewMaterializeTargetService.prune_terminal_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-materializer:a-review0-materializer-reviewmaterializet-6836c7e715:9131d697ba -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d0840900b9"></a>
- <a id="s-e69387b2fc"></a>`distribution`: `a-review0-materializer`
- <a id="s-3feedc8cbd"></a>`module`: `a_review0_materializer`
- <a id="s-8a8a399604"></a>`name`: `prune_terminal_state`
- <a id="s-d3b7b88b7e"></a>`owner`: `a_review0_materializer.ReviewMaterializeTargetService`
- <a id="s-4cddc7c75e"></a>`unit`: `member`

### Declared structure

- <a id="s-15c44e15dd"></a>`kind`: `"method"`
- <a id="s-d86f1d6223"></a>`signature`: `"\"(self, *, now: 'float \| None' = None) -> 'dict[str, int]'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](a-review0-materializer-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-4692155fbb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-materializer:a_review0_materializer](../../../evidence/sources/authorities.md#src-fa69f32f84) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_materializer.ReviewMaterializeTargetService.prune_terminal_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb7b390b1eaaa2380567204d629d69c933b4795bc6d966256d6d0e5458104ca4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, now: 'float | None' = None) -> 'dict[str, int]'\""
  },
  "distribution": "a-review0-materializer",
  "module": "a_review0_materializer",
  "name": "prune_terminal_state",
  "owner": "a_review0_materializer.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
