# stove0_core.PreviewAcceptance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-previewacceptance:66d26f886a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-be8dfb6f49"></a>
| Field | Shape |
|---|---|
| <a id="s-396c55f71f"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0edf74d29c"></a>`distribution` | "stove0-server" |
| <a id="s-b61610a0b6"></a>`module` | "stove0_core" |
| <a id="s-d072a55126"></a>`name` | "PreviewAcceptance" |
| <a id="s-64ded84d2d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.PreviewAcceptance.canonical_targets](stove0-core-previewacceptance-canonical-targets.md)
- [stove0_core.PreviewAcceptance.from_preview](stove0-core-previewacceptance-from-preview.md)

## Governing policies

- <a id="pa-9059b40341"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.PreviewAcceptance`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d5a3404abd8a6a7ec552a6496cd1bd0584780ff872ac9bb52a0fa14e5f996dc -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "20a99f60eb42afdb3b4dcf73b8a47e422e2b26c60fdb64e420990ff78c34da8c",
    "signature": "\"(*, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_plans: tuple[stove0_core.work_state.PreviewTargetExpectation, ...]) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "PreviewAcceptance",
  "unit": "export"
}
```
