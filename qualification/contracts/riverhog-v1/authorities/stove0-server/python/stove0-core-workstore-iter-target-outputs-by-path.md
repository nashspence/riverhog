# stove0_core.WorkStore.iter_target_outputs_by_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-iter-target-outputs-by-path:56b183a115 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f5fc54eca1"></a>
- <a id="s-79d498fc56"></a>`distribution`: `stove0-server`
- <a id="s-deef6f34c9"></a>`module`: `stove0_core`
- <a id="s-fbd928e77b"></a>`name`: `iter_target_outputs_by_path`
- <a id="s-e735cb436f"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-60a632c242"></a>`unit`: `member`

### Declared structure

- <a id="s-716678db97"></a>`kind`: `"method"`
- <a id="s-96b5ee6440"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-e07afa5b85"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.iter_target_outputs_by_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae3375993d09cd74496f15822b2a264545a904ef9d91cce5055ba61a5707c5a1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_outputs_by_path",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
