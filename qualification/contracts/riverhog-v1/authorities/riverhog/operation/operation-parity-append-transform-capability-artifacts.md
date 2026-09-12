# Operation parity: append_transform_capability_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-append-transform-capabil-b581c14405:e34d163316 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-240ab7f2d4"></a>
| Concern | Contract |
|---|---|
| <a id="s-252fa50fc7"></a>`application` | riverhog |
| <a id="s-360a9ba3ec"></a>`classification` | client-only-primitive |
| <a id="s-44f57c1851"></a>`cli_commands` | [] |
| <a id="s-be2a31308c"></a>`client` | ApiClient |
| <a id="s-8d7165ce67"></a>`method` | PUT |
| <a id="s-4eed8dbaf7"></a>`operation_id` | append_transform_capability_artifacts |
| <a id="s-8aabef2e02"></a>`path` | /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts |
| <a id="s-500a7b6392"></a>`provider_evidence` | None |
| <a id="s-174b50f3d2"></a>`read_collection` | None |
| <a id="s-44c56d2d4a"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts](../http/put-v1-collection-processing-claims-claim-id-capabilities-capability-id-artifacts.md)

## Governing policies

- <a id="pa-e4fe7b58fe"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-df535acf55"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-c2390e46c2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/29`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 025a9f977d9d40474ee72cc381d4fe447d833f8274d29cdd9f8932ff9b247a8f -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "append_transform_capability_artifacts",
  "path": "/v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
