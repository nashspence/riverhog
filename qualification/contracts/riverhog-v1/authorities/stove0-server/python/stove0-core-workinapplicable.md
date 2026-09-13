# stove0_core.WorkInapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workinapplicable:35a55644c3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e895c0cf56"></a>
| Field | Shape |
|---|---|
| <a id="s-82cf1aa770"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-1932af15f3"></a>`distribution` | "stove0-server" |
| <a id="s-0e6fafe095"></a>`module` | "stove0_core" |
| <a id="s-7f1b51dd48"></a>`name` | "WorkInapplicable" |
| <a id="s-27f12592d0"></a>`unit` | "export" |

## Governing policies

- <a id="pa-0646eb3115"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkInapplicable`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 04f24942807d58d436fc6712e8e1f6a431479eaecf27a3d29424df390fd3be65 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "842871cfa20a4e6af7501b4980d0c168945e538de5f72d10eaa49ebb577909cb",
    "signature": "'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "WorkInapplicable",
  "unit": "export"
}
```
