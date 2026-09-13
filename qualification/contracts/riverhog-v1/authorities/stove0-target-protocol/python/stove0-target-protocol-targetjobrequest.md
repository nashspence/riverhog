# stove0_target_protocol.TargetJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobrequest:dc2711e32c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b03dff792f"></a>
| Field | Shape |
|---|---|
| <a id="s-0337737fb3"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-eef13811c0"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-c98e03ec89"></a>`module` | "stove0_target_protocol" |
| <a id="s-d9131e81fc"></a>`name` | "TargetJobRequest" |
| <a id="s-6f74b4fc74"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetJobRequest.accepted](stove0-target-protocol-targetjobrequest-accepted.md)
- [stove0_target_protocol.TargetJobRequest.seal](stove0-target-protocol-targetjobrequest-seal.md)
- [stove0_target_protocol.TargetJobRequest.verify_digest](stove0-target-protocol-targetjobrequest-verify-digest.md)

## Governing policies

- <a id="pa-db8081f8df"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa0620dc9d9ade2bfda5b793934b397fa09546d9247de8afdf95fe9c99de20b8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a9548cb0b668b182a9fdebb904bc5e91d9299131bbf9c38cff8a5c19c450063e",
    "signature": "\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, runtime: stove0_target_protocol.protocol.TargetRuntimeAuthority, callback_access: stove0_target_protocol.protocol.TargetCallbackAccess, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetJobRequest",
  "unit": "export"
}
```
