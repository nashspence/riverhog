# stove0_review_target_support.ReviewTargetServiceBase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtargetservicebase:7d1371a82e -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-256bced623"></a>`signature`: `"\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', protocol: 'TargetProtocol', implementation_id: 'str', operation: 'OperationContract', options_schema: 'JsonSchemaDocument', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [preflight](stove0-review-target-support-reviewtargetservicebase-preflight.md)
- [readiness](stove0-review-target-support-reviewtargetservicebase-readiness.md)
- [close](stove0-review-target-support-reviewtargetservicebase-close.md)

## Governing policies

- <a id="pa-81269187c3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetServiceBase`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e3e3c980bcb219c971adab693907f879fded893a53b2097080e955b066018f3 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', protocol: 'TargetProtocol', implementation_id: 'str', operation: 'OperationContract', options_schema: 'JsonSchemaDocument', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "ReviewTargetServiceBase",
  "unit": "export"
}
```
