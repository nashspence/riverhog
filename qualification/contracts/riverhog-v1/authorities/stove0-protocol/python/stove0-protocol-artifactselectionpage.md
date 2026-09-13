# stove0_protocol.ArtifactSelectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselectionpage:e313fe515e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cce11045fd"></a>
| Field | Shape |
|---|---|
| <a id="s-616596d88e"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-56d70c655b"></a>`distribution` | "stove0-protocol" |
| <a id="s-03e18b6dc0"></a>`module` | "stove0_protocol" |
| <a id="s-61c8bc0d62"></a>`name` | "ArtifactSelectionPage" |
| <a id="s-ac3d36c57f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ArtifactSelectionPage.bind_page](stove0-protocol-artifactselectionpage-bind-page.md)

## Governing policies

- <a id="pa-eb1a09b97a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelectionPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ccfb22d92a907fc9a80e0cdb093bf416514fe016d049f9cfdbee7b706f6d79c1 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ab841b1329af4b9af6b96b651097d0aadeaf9e86a6033250b4a5a4a5f9d9712c",
    "signature": "\"(*, authority: stove0_protocol.fork_join.ArtifactSelectionRef, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MaxLen(max_length=256)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ArtifactSelectionPage",
  "unit": "export"
}
```
