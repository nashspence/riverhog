# stove0_protocol.WorkIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workidentity:41554d3c31 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a9355fab3f"></a>
| Field | Shape |
|---|---|
| <a id="s-544c414973"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-21e699da7a"></a>`distribution` | "stove0-protocol" |
| <a id="s-f2c1899532"></a>`module` | "stove0_protocol" |
| <a id="s-09cef259be"></a>`name` | "WorkIdentity" |
| <a id="s-6a6f9ccdaf"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkIdentity.root_identities](stove0-protocol-workidentity-root-identities.md)
- [stove0_protocol.WorkIdentity.seal](stove0-protocol-workidentity-seal.md)
- [stove0_protocol.WorkIdentity.verify_digest](stove0-protocol-workidentity-verify-digest.md)

## Governing policies

- <a id="pa-bd606aa428"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6b4d6a5b5e6b31d74be492884d25704431b72c0b4ab61ea33346a450c3abbaa -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "23995cc54c7aa6ac8e45d8e3eeaedcd6d3ecf87ea8a383e5ffa294f37b01e0c4",
    "signature": "\"(*, format: Literal['stove0-work/v1'] = 'stove0-work/v1', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, evaluation: stove0_protocol.models.EvaluationBinding | None = None, fork_join: Optional[Annotated[stove0_protocol.models.BranchWorkBinding | stove0_protocol.models.JoinWorkBinding, FieldInfo(annotation=NoneType, required=True, discriminator='kind')]] = None, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkIdentity",
  "unit": "export"
}
```
