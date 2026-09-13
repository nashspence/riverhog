# stove0_core.WorkStore.target_source_edge_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-target-source-edge-page:cff4a4ae42 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fe23ae84be"></a>
| Field | Shape |
|---|---|
| <a id="s-dcfe97c1a3"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b0cdb4e63d"></a>`distribution` | "stove0-server" |
| <a id="s-f9c97418c8"></a>`module` | "stove0_core" |
| <a id="s-92d4e22c86"></a>`name` | "target_source_edge_page" |
| <a id="s-d0b3a9c525"></a>`owner` | "stove0_core.WorkStore" |
| <a id="s-6b2b2ef9c7"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-11b55ffa05"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.target_source_edge_page`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ba75e8913b4cec0c06d81e02399d718653d28518d733789e38341a3523da2e8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "'(self, work_id: \\'str\\', job_id: \\'str\\', *, order: \"Literal[\\'output\\', \\'input\\']\", after_output_id: \\'str | None\\', after_input_id: \\'str | None\\', limit: \\'int\\') -> \\'tuple[OutputSourceEdge, ...]\\''"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_source_edge_page",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
