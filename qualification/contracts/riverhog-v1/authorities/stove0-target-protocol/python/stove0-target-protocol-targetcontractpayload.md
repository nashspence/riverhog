# stove0_target_protocol.TargetContractPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcontractpayload:74fc9d5686 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-af1a0e9066"></a>
| Field | Shape |
|---|---|
| <a id="s-6105342c5c"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c853004fc0"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-5851fe30d4"></a>`module` | "stove0_target_protocol" |
| <a id="s-4b92e3e744"></a>`name` | "TargetContractPayload" |
| <a id="s-9cf33c1ccf"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetContractPayload.bind_result_kind](stove0-target-protocol-targetcontractpayload-bind-result-kind.md)
- [stove0_target_protocol.TargetContractPayload.canonical_operations](stove0-target-protocol-targetcontractpayload-canonical-operations.md)

## Governing policies

- <a id="pa-ddbe730940"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetContractPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34aaf9e92425891bc72d9be1d1f60b18b2bad4e64e9ad472665ea6f95f9eb418 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ee590c3be91952c47c3452289eef103fef83febe06ce4775481be63c2b40d821",
    "signature": "\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetContractPayload",
  "unit": "export"
}
```
