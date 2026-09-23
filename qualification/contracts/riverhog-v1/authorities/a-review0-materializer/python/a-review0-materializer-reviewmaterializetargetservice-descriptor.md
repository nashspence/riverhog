# a_review0_materializer.ReviewMaterializeTargetService.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-materializer:a-review0-materializer-reviewmaterializet-0cf06b9f1c:9bf6babeca -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c30336cae9"></a>
- <a id="s-92b0ec803a"></a>`distribution`: `a-review0-materializer`
- <a id="s-e8b2a7d59b"></a>`module`: `a_review0_materializer`
- <a id="s-0d148d7e4a"></a>`name`: `descriptor`
- <a id="s-59fb08f009"></a>`owner`: `a_review0_materializer.ReviewMaterializeTargetService`
- <a id="s-78ea244794"></a>`unit`: `member`

### Declared structure

- <a id="s-fb0b6723f3"></a>`kind`: `"method"`
- <a id="s-1c55e7e8bd"></a>`signature`: `"\"(self) -> 'TargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [ReviewMaterializeTargetService](a-review0-materializer-reviewmaterializetargetservice.md)

## Governing policies

- <a id="pa-6932d1ffff"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-materializer:a_review0_materializer](../../../evidence/sources/authorities.md#src-fa69f32f84) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_materializer.ReviewMaterializeTargetService.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ec264b0e7b04bad477f425dd3c69f48fc411f08700e31d5cdbabd2e5f05a5e7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetDescriptor'\""
  },
  "distribution": "a-review0-materializer",
  "module": "a_review0_materializer",
  "name": "descriptor",
  "owner": "a_review0_materializer.ReviewMaterializeTargetService",
  "unit": "member"
}
```

</details>
