# stove0_core.CoordinationProjection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-coordinationprojection:eb01f3cf8d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-959e678a6d"></a>
- <a id="s-9357d4b066"></a>`distribution`: `stove0-server`
- <a id="s-d631cf993d"></a>`module`: `stove0_core`
- <a id="s-554088950f"></a>`name`: `CoordinationProjection`
- <a id="s-e3270c1173"></a>`unit`: `export`

### Declared structure

- <a id="s-99b4336ad2"></a>`kind`: `"class"`
- <a id="s-8674c23b32"></a>`signature`: `"\"(evaluation: 'BranchSetEvaluation', selection_documents: 'dict[str, ArtifactSelection]', pending_join: 'JoinPlan \| None' = None, pending_join_selections: 'tuple[ArtifactSelection, ...]' = ()) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-dd5fe55969"></a>`evaluation` | `'BranchSetEvaluation'` | `required` |
| <a id="s-e6f7a3bae5"></a>`selection_documents` | `'dict[str, ArtifactSelection]'` | `required` |
| <a id="s-864264fa87"></a>`pending_join` | `'JoinPlan \| None'` | `None` |
| <a id="s-431426261c"></a>`pending_join_selections` | `'tuple[ArtifactSelection, ...]'` | `()` |

## Governing policies

- <a id="pa-484eecf41a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.CoordinationProjection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9b17b2d25aa3de47e75074c1db06c034d273158a69e5c6405a10a2147097f41 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "evaluation",
        "type": "'BranchSetEvaluation'"
      },
      {
        "default": "required",
        "name": "selection_documents",
        "type": "'dict[str, ArtifactSelection]'"
      },
      {
        "default": "None",
        "name": "pending_join",
        "type": "'JoinPlan | None'"
      },
      {
        "default": "()",
        "name": "pending_join_selections",
        "type": "'tuple[ArtifactSelection, ...]'"
      }
    ],
    "kind": "class",
    "signature": "\"(evaluation: 'BranchSetEvaluation', selection_documents: 'dict[str, ArtifactSelection]', pending_join: 'JoinPlan | None' = None, pending_join_selections: 'tuple[ArtifactSelection, ...]' = ()) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "CoordinationProjection",
  "unit": "export"
}
```

</details>
