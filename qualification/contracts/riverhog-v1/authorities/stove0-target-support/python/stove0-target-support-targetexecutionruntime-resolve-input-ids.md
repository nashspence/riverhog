# stove0_target_support.TargetExecutionRuntime.resolve_input_ids

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionrunt-6d60dbc410:463f484a17 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e462b31615"></a>
- <a id="s-f1e6d94f94"></a>`distribution`: `stove0-target-support`
- <a id="s-afaa700ebd"></a>`module`: `stove0_target_support`
- <a id="s-3359ad4488"></a>`name`: `resolve_input_ids`
- <a id="s-49652583c4"></a>`owner`: `stove0_target_support.TargetExecutionRuntime`
- <a id="s-04d7cc9732"></a>`unit`: `member`

### Declared structure

- <a id="s-3ad2f929c9"></a>`kind`: `"method"`
- <a id="s-7106ab15dc"></a>`signature`: `"\"(self, input_ids: 'Sequence[str]') -> 'tuple[ClaimedArtifact, ...]'\""`

## Maintained corroboration

### Related interface records

- [TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-bb787c7df4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.resolve_input_ids`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a8c97646d5fb7baa88c15d981390fea1f533a29544c50681c6c78d23910a252d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, input_ids: 'Sequence[str]') -> 'tuple[ClaimedArtifact, ...]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "resolve_input_ids",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```

</details>
