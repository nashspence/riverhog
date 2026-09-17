# stove0_core.WorkStore.iter_target_source_edges

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-iter-target-source-edges:8bce9822fb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c6486bb49a"></a>
- <a id="s-70725c5d3c"></a>`distribution`: `stove0-server`
- <a id="s-8d2d011ea6"></a>`module`: `stove0_core`
- <a id="s-99293529c8"></a>`name`: `iter_target_source_edges`
- <a id="s-35c605d846"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-8ca1ea56a0"></a>`unit`: `member`

### Declared structure

- <a id="s-970229d2b1"></a>`kind`: `"method"`
- <a id="s-54d67b65db"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-0d63bd6da7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.iter_target_source_edges`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df66a53ede0dedda03ab60a353df77f3504999b0a78c7e6c3891f7e1b7451662 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_source_edges",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
