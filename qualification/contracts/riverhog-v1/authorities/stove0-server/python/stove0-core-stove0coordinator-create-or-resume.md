# stove0_core.Stove0Coordinator.create_or_resume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0coordinator-create-or-resume:e291f2ae0a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-84d4b4ea57"></a>
- <a id="s-61e4e0422e"></a>`distribution`: `stove0-server`
- <a id="s-c8661522ee"></a>`module`: `stove0_core`
- <a id="s-3f4ec09b1b"></a>`name`: `create_or_resume`
- <a id="s-e18cf22bc7"></a>`owner`: `stove0_core.Stove0Coordinator`
- <a id="s-84c7225c4b"></a>`unit`: `member`

### Declared structure

- <a id="s-56778a6d36"></a>`kind`: `"method"`
- <a id="s-a69d197227"></a>`signature`: `"\"(self, identity: 'WorkIdentity', *, preview: 'WorkflowPreview \| None' = None) -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0Coordinator](stove0-core-stove0coordinator.md)

## Governing policies

- <a id="pa-0ea5242575"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0Coordinator.create_or_resume`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a6ddb368a8c36821e474bc4bf81d6dab4327c9568314f8b0a849bccffb164a84 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, identity: 'WorkIdentity', *, preview: 'WorkflowPreview | None' = None) -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create_or_resume",
  "owner": "stove0_core.Stove0Coordinator",
  "unit": "member"
}
```

</details>
