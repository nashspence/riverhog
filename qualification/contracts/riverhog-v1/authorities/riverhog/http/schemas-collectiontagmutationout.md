# schemas: CollectionTagMutationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiontagmutationout:6300d19241 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTagMutationOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: CollectionTagMutationOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `action` | yes | string |  |
| `changed` | yes | boolean |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `head_identity` | yes | string |  |
| `operation_id` | yes | string |  |
| `revision` | yes | integer |  |
| `root_sha256` | yes | object (2 fields) |  |
| `state` | yes | string |  |
| `tag` | yes | #/components/schemas/CollectionTag |  |
| `tag_set_identity` | yes | string |  |
