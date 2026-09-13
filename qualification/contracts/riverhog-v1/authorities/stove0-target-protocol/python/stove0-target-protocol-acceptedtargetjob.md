# stove0_target_protocol.AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-acceptedtargetjob:624de7b1d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c392c78ff8"></a>
| Field | Shape |
|---|---|
| <a id="s-194fc960e2"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7607609177"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-d6419fb725"></a>`module` | "stove0_target_protocol" |
| <a id="s-449903f6ce"></a>`name` | "AcceptedTargetJob" |
| <a id="s-9ff0bccf11"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.AcceptedTargetJob.verify_digest](stove0-target-protocol-acceptedtargetjob-verify-digest.md)

## Governing policies

- <a id="pa-43e2eb2826"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.AcceptedTargetJob`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 126921afb94eda052f87f15202770551b23f36ed8ddcd5a1233ce71f3936c132 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d6edef74a94d6ff90c51b59d72f6c750c6e411f0d80816c3136379e41e828867",
    "signature": "\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "AcceptedTargetJob",
  "unit": "export"
}
```
