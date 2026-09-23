# stove0_review_target_support.ReviewTargetServiceBase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtargetservicebase:7d1371a82e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1bf3303742"></a>
- <a id="s-42801fdee1"></a>`distribution`: `stove0-review-target-support`
- <a id="s-3b3786718d"></a>`module`: `stove0_review_target_support`
- <a id="s-af0176407b"></a>`name`: `ReviewTargetServiceBase`
- <a id="s-aa5ac1fc7c"></a>`unit`: `export`

### Declared structure

- <a id="s-156920b352"></a>`kind`: `"class"`
- <a id="s-256bced623"></a>`signature`: `"\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', protocol: 'TargetProtocol', implementation_id: 'str', operation: 'OperationContract', options_schema: 'JsonSchemaValidationProfile', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [contract](stove0-review-target-support-reviewtargetservicebase-contract.md)
- [prune_terminal_state](stove0-review-target-support-reviewtargetservicebase-prune-terminal-state.md)
- [cancel_job](stove0-review-target-support-reviewtargetservicebase-cancel-job.md)
- [preflight](stove0-review-target-support-reviewtargetservicebase-preflight.md)
- [get_job](stove0-review-target-support-reviewtargetservicebase-get-job.md)
- [put_job](stove0-review-target-support-reviewtargetservicebase-put-job.md)
- [readiness](stove0-review-target-support-reviewtargetservicebase-readiness.md)
- [close](stove0-review-target-support-reviewtargetservicebase-close.md)

## Governing policies

- <a id="pa-81269187c3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources/authorities.md#src-2a89a71c41) — [reference/stove0/targets/review/support/src/stove0\_review\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetServiceBase`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7fee6c22fde9b022cd3ec14b26868caae1ccaf32111821808f1fe24bb1c35d9e -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', protocol: 'TargetProtocol', implementation_id: 'str', operation: 'OperationContract', options_schema: 'JsonSchemaValidationProfile', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "ReviewTargetServiceBase",
  "unit": "export"
}
```

</details>
