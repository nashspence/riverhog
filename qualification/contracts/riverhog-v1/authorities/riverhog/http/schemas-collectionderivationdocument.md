# schemas: CollectionDerivationDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionderivationdocument:78d3d731d2 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 8 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDerivationDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
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
| encoded-size | bytes | `contract_max` | maximum=16777216, reason=bounded-controller-evidence-envelope |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: CollectionDerivationDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_set_sha256` | yes | string |  |
| `claim` | yes | #/components/schemas/ClaimFenceDocument |  |
| `controller_evidence` | yes | object |  |
| `controller_evidence_sha256` | yes | string |  |
| `disposition_set` | yes | #/components/schemas/ArtifactDispositionSetIdentityDocument |  |
| `execution_envelope_sha256` | yes | string |  |
| `execution_id` | yes | string |  |
| `execution_sha256` | yes | string |  |
| `format` | yes | string |  |
| `input_set_sha256` | yes | string |  |
| `operation` | yes | #/components/schemas/OperationIdentityDocument |  |
| `recipe` | yes | #/components/schemas/RecipeIdentityDocument |  |
