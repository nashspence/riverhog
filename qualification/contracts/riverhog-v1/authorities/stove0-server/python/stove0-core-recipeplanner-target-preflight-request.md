# stove0_core.RecipePlanner.target_preflight_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipeplanner-target-preflight-request:d24138a325 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-25c895314b"></a>
- <a id="s-96f6782021"></a>`distribution`: `stove0-server`
- <a id="s-3c73ce6c03"></a>`module`: `stove0_core`
- <a id="s-0e2d58afa3"></a>`name`: `target_preflight_request`
- <a id="s-7ec1c45862"></a>`owner`: `stove0_core.RecipePlanner`
- <a id="s-6b64f650c5"></a>`unit`: `member`

### Declared structure

- <a id="s-ad95bfdf6a"></a>`kind`: `"method"`
- <a id="s-cf935e889a"></a>`signature`: `"\"(self, plan: 'WorkflowPlan', selections: 'Mapping[str, ArtifactSelection]') -> 'TargetPreflightRequest'\""`

## Maintained corroboration

### Related interface records

- [RecipePlanner](stove0-core-recipeplanner.md)

## Governing policies

- <a id="pa-9d35200c1e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipePlanner.target_preflight_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5755a98791f3f5580294c52a2e1f0d778f2d655298f63a4c8c971a535cc61475 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan: 'WorkflowPlan', selections: 'Mapping[str, ArtifactSelection]') -> 'TargetPreflightRequest'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_preflight_request",
  "owner": "stove0_core.RecipePlanner",
  "unit": "member"
}
```

</details>
