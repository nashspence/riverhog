# stove0_target_support.AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-acceptedtargetjob:f4887db677 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b852337104"></a>
| Field | Shape |
|---|---|
| <a id="s-631ad4c287"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-e9e2b14fd3"></a>`distribution` | "stove0-target-support" |
| <a id="s-20411255f8"></a>`module` | "stove0_target_support" |
| <a id="s-ebebba62f6"></a>`name` | "AcceptedTargetJob" |
| <a id="s-68775f8cff"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.AcceptedTargetJob.verify_digest](stove0-target-support-acceptedtargetjob-verify-digest.md)

## Governing policies

- <a id="pa-39a0a37e45"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.AcceptedTargetJob`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f1a0c4ae335efe06af8438c1f7ce3a826174ddababe745ac828c5a7d606d9242 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d6edef74a94d6ff90c51b59d72f6c750c6e411f0d80816c3136379e41e828867",
    "signature": "\"(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "AcceptedTargetJob",
  "unit": "export"
}
```
