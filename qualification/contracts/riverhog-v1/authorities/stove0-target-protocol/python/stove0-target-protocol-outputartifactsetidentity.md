# stove0_target_protocol.OutputArtifactSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputartifactsetidentity:471701b4b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2abc66a0f6"></a>
| Field | Shape |
|---|---|
| <a id="s-6630dac3da"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-8c70c70601"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-c2b2a0df19"></a>`module` | "stove0_target_protocol" |
| <a id="s-095e1a5956"></a>`name` | "OutputArtifactSetIdentity" |
| <a id="s-d0a43c4ac2"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OutputArtifactSetIdentity.validate_summary](stove0-target-protocol-outputartifactsetidentity-validate-summary.md)
- [stove0_target_protocol.OutputArtifactSetIdentity.seal](stove0-target-protocol-outputartifactsetidentity-seal.md)
- [stove0_target_protocol.OutputArtifactSetIdentity.seal_iterable](stove0-target-protocol-outputartifactsetidentity-seal-iterable.md)

## Governing policies

- <a id="pa-7fcd66579e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputArtifactSetIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b488c6f10315425b0426ff3dcb55bba9d85f900f423b02f4814f053641ef30d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "75c833d938ce913e50ba083b5326c6ddf408d2de5335bbb1872610b2c298dcbe",
    "signature": "\"(*, artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)], roles: Annotated[tuple[stove0_target_protocol.protocol.OutputArtifactRoleCount, ...], MinLen(min_length=1)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "OutputArtifactSetIdentity",
  "unit": "export"
}
```
