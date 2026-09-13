# stove0_target_protocol.TargetInputPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetinputpage:b5c9613dd6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-412205faec"></a>
| Field | Shape |
|---|---|
| <a id="s-63207abbb7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-22e6c8749a"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-d46c396912"></a>`module` | "stove0_target_protocol" |
| <a id="s-640b511f35"></a>`name` | "TargetInputPage" |
| <a id="s-d8bd36b213"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetInputPage.bind_page](stove0-target-protocol-targetinputpage-bind-page.md)

## Governing policies

- <a id="pa-bb4fb25791"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetInputPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3555b2f8f4217b95be4634e7174c83678c96b19dd0c28c89139aac7534f6c31 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7131a1bfdc14187626d6da1e2ed0c2ab5386ccb022b71b5a173812612214dcb0",
    "signature": "\"(*, authority: stove0_target_protocol.protocol.TargetInputAuthority, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_target_protocol.protocol.InputArtifact, ...], MaxLen(max_length=256)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetInputPage",
  "unit": "export"
}
```
