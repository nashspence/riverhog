# review0_target_lib.ReviewTargetServiceBase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-reviewtargetservicebase:d2a83c5547 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc054c5f14"></a>
- <a id="s-361949f599"></a>`distribution`: `review0-target-lib`
- <a id="s-765928adf7"></a>`module`: `review0_target_lib`
- <a id="s-320a0fca8b"></a>`name`: `ReviewTargetServiceBase`
- <a id="s-d394f2e036"></a>`unit`: `export`

### Declared structure

- <a id="s-4d913c6dda"></a>`kind`: `"class"`
- <a id="s-9e9dfc4ea1"></a>`signature`: `"\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_id: 'str', implementation_version: 'str', protocol: 'TargetProtocol', implementation_id: 'str', operation: 'OperationContract', options_schema: 'JsonSchemaValidationProfile', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [cancel_job](review0-target-lib-reviewtargetservicebase-cancel-job.md)
- [descriptor](review0-target-lib-reviewtargetservicebase-descriptor.md)
- [prune_terminal_state](review0-target-lib-reviewtargetservicebase-prune-terminal-state.md)
- [close](review0-target-lib-reviewtargetservicebase-close.md)
- [get_job](review0-target-lib-reviewtargetservicebase-get-job.md)
- [preflight](review0-target-lib-reviewtargetservicebase-preflight.md)
- [put_job](review0-target-lib-reviewtargetservicebase-put-job.md)
- [readiness](review0-target-lib-reviewtargetservicebase-readiness.md)

## Governing policies

- <a id="pa-538e0debfb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.ReviewTargetServiceBase`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ad11c1f6f9003e1154a7709b22b65dbb4a4da50b94174fa84b15db4372865ef -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_id: 'str', implementation_version: 'str', protocol: 'TargetProtocol', implementation_id: 'str', operation: 'OperationContract', options_schema: 'JsonSchemaValidationProfile', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "ReviewTargetServiceBase",
  "unit": "export"
}
```

</details>
