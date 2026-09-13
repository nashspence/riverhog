# stove0_core.PreviewTargetExpectation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-previewtargetexpectation:f827a4d8e4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5540c981d8"></a>
| Field | Shape |
|---|---|
| <a id="s-120989f5bc"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-f66bc4e9c2"></a>`distribution` | "stove0-server" |
| <a id="s-9a24fe911e"></a>`module` | "stove0_core" |
| <a id="s-0fb1f46469"></a>`name` | "PreviewTargetExpectation" |
| <a id="s-f3b7ee74f8"></a>`unit` | "export" |

## Governing policies

- <a id="pa-4b65831db1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.PreviewTargetExpectation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 381a543baf61a29904c46d471532a047d8b07696736bf92e6d4e9beed37185b4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5a61cd78514147734c19e5793fffabbafdb2c06e87e48d8a737ef672985c25ac",
    "signature": "\"(*, branch_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "PreviewTargetExpectation",
  "unit": "export"
}
```
