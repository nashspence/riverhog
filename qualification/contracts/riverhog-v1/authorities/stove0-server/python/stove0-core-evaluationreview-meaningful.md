# stove0_core.EvaluationReview.meaningful

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationreview-meaningful:698235bbdc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-15fd97048b"></a>
- <a id="s-bbd5a1c728"></a>`distribution`: `stove0-server`
- <a id="s-e498c31cfe"></a>`module`: `stove0_core`
- <a id="s-7b22b24a7d"></a>`name`: `meaningful`
- <a id="s-f5e4eb10e0"></a>`owner`: `stove0_core.EvaluationReview`
- <a id="s-5568afd1d3"></a>`unit`: `member`

### Declared structure

- <a id="s-1fa966dc34"></a>`kind`: `"method"`
- <a id="s-736338fc3a"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [EvaluationReview](stove0-core-evaluationreview.md)

## Governing policies

- <a id="pa-9434cd1ebe"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationReview.meaningful`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac5c3fdc3ae1b98ea65699ecdc58bf8b17c34fb19996c3ebe78fd09d513fa6bf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "meaningful",
  "owner": "stove0_core.EvaluationReview",
  "unit": "member"
}
```

</details>
