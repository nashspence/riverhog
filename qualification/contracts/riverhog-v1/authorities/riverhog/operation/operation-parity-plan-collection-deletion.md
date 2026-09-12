# Operation parity: plan_collection_deletion

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-plan-collection-deletion:f1f60b6166 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ab4e25ac94b6"></a>
| Concern | Contract |
|---|---|
| <a id="s-9806e904ff9c"></a>`application` | riverhog |
| <a id="s-f823abb2f050"></a>`classification` | human-cli+json |
| <a id="s-306acc3739cb"></a>`cli_commands` | ["collection delete"] |
| <a id="s-2b2b0f01ab32"></a>`client` | ApiClient |
| <a id="s-bc410ace1933"></a>`method` | POST |
| <a id="s-54fa0d5d4d78"></a>`operation_id` | plan_collection_deletion |
| <a id="s-29f1f7a25932"></a>`path` | /v1/collections/{collection_id}/deletion-plan |
| <a id="s-a507fa6ab79f"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-c9170a66e227"></a>`read_collection` | None |
| <a id="s-49e0645ffbb2"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collections/{collection_id}/deletion-plan](../http/post-v1-collections-collection-id-deletion-plan.md)
- [piggity collection delete](../../piggity/cli/piggity-collection-delete.md)

## Governing policies

- <a id="pa-d752b1cf81f6"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-e81ae5357c56"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-6c0933dcd755"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/74`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf53e47a8c21a41ac5ba3b8b95b64803fcbd11c90882fad52157af9112495bc5 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection delete"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "plan_collection_deletion",
  "path": "/v1/collections/{collection_id}/deletion-plan",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
