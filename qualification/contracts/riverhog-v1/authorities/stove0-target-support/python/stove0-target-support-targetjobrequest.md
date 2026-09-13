# stove0_target_support.TargetJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobrequest:8abb832635 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3cc758c929"></a>
| Field | Shape |
|---|---|
| <a id="s-c2f7eb8250"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7f57d04aa1"></a>`distribution` | "stove0-target-support" |
| <a id="s-5101141394"></a>`module` | "stove0_target_support" |
| <a id="s-2990e7eb13"></a>`name` | "TargetJobRequest" |
| <a id="s-4f01aa6e2f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetJobRequest.accepted](stove0-target-support-targetjobrequest-accepted.md)
- [stove0_target_support.TargetJobRequest.seal](stove0-target-support-targetjobrequest-seal.md)
- [stove0_target_support.TargetJobRequest.verify_digest](stove0-target-support-targetjobrequest-verify-digest.md)

## Governing policies

- <a id="pa-1d82f531c1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b776f0de60e8baa3b755b7f2c1a1719ed4473e6ae7c70b1915ec9d6e24fde4e -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a9548cb0b668b182a9fdebb904bc5e91d9299131bbf9c38cff8a5c19c450063e",
    "signature": "\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, runtime: stove0_target_protocol.protocol.TargetRuntimeAuthority, callback_access: stove0_target_protocol.protocol.TargetCallbackAccess, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetJobRequest",
  "unit": "export"
}
```
