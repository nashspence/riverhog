# stove0_target_support.TargetContractPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcontractpayload:ccbffdf823 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4de55964a6"></a>
| Field | Shape |
|---|---|
| <a id="s-c121585417"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-518911b948"></a>`distribution` | "stove0-target-support" |
| <a id="s-51e0b64c52"></a>`module` | "stove0_target_support" |
| <a id="s-b2c37db9f0"></a>`name` | "TargetContractPayload" |
| <a id="s-d55b8d75cc"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetContractPayload.bind_result_kind](stove0-target-support-targetcontractpayload-bind-result-kind.md)
- [stove0_target_support.TargetContractPayload.canonical_operations](stove0-target-support-targetcontractpayload-canonical-operations.md)

## Governing policies

- <a id="pa-4f06eb5170"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetContractPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7828f1a4db15c11f1a680fd32cc18b988f1d2732dcaafaee49623dea9bda6c8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ee590c3be91952c47c3452289eef103fef83febe06ce4775481be63c2b40d821",
    "signature": "\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetContractPayload",
  "unit": "export"
}
```
