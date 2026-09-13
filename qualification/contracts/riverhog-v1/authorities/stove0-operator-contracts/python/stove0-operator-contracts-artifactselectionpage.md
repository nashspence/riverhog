# stove0_operator_contracts.ArtifactSelectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-artifactselectionpage:cfac71f202 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b4744a6bbb"></a>
| Field | Shape |
|---|---|
| <a id="s-cc962486ff"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6f18d8a8ef"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-681fc212d5"></a>`module` | "stove0_operator_contracts" |
| <a id="s-0467b1777a"></a>`name` | "ArtifactSelectionPage" |
| <a id="s-78451bb7b2"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.ArtifactSelectionPage.bind_page](stove0-operator-contracts-artifactselectionpage-bind-page.md)

## Governing policies

- <a id="pa-8210bec093"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.ArtifactSelectionPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 45f3d1f0f1be7e7c3ebf5df690a239761d16aac8f82fee2bd4b73dc61a60affd -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ab841b1329af4b9af6b96b651097d0aadeaf9e86a6033250b4a5a4a5f9d9712c",
    "signature": "\"(*, authority: stove0_protocol.fork_join.ArtifactSelectionRef, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MaxLen(max_length=256)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "ArtifactSelectionPage",
  "unit": "export"
}
```
