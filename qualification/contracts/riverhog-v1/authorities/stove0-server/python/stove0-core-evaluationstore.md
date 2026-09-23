# stove0_core.EvaluationStore

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationstore:a7737441d7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c112edf5fb"></a>
- <a id="s-015912d3c0"></a>`distribution`: `stove0-server`
- <a id="s-da0b039441"></a>`module`: `stove0_core`
- <a id="s-367c977951"></a>`name`: `EvaluationStore`
- <a id="s-974501158c"></a>`unit`: `export`

### Declared structure

- <a id="s-417ba65f80"></a>`kind`: `"class"`
- <a id="s-f0843dbadc"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [compare_and_swap](stove0-core-evaluationstore-compare-and-swap.md)
- [create](stove0-core-evaluationstore-create.md)
- [load](stove0-core-evaluationstore-load.md)

## Governing policies

- <a id="pa-565c6a6f12"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationStore`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0be2c8936e0d7189022247aa9fef1a233fe16f3ba6a2dd44a6fd7ee313acb9fc -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "EvaluationStore",
  "unit": "export"
}
```

</details>
