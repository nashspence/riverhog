# stove0_core.EvaluationChild.validate_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationchild-validate-output:eb9eec9542 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-50d2a0ce1a"></a>
- <a id="s-5dda24a726"></a>`distribution`: `stove0-server`
- <a id="s-1876e25553"></a>`module`: `stove0_core`
- <a id="s-ddecf042e6"></a>`name`: `validate_output`
- <a id="s-04a892c365"></a>`owner`: `stove0_core.EvaluationChild`
- <a id="s-a150bd8ea7"></a>`unit`: `member`

### Declared structure

- <a id="s-34bacc389e"></a>`kind`: `"method"`
- <a id="s-89c49c70a4"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [EvaluationChild](stove0-core-evaluationchild.md)

## Governing policies

- <a id="pa-6c69f5d9af"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationChild.validate_output`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88d31a58c752265287a2f279a0cb27bc7d7db0cc0d19a1974ee341463e061102 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "validate_output",
  "owner": "stove0_core.EvaluationChild",
  "unit": "member"
}
```

</details>
