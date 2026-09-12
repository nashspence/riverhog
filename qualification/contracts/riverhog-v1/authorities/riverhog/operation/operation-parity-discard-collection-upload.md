# Operation parity: discard_collection_upload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-discard-collection-upload:a66ff760ff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-38da75f4f260"></a>
| Concern | Contract |
|---|---|
| <a id="s-b9499ec46c3c"></a>`application` | riverhog |
| <a id="s-bf0932ed1166"></a>`classification` | human-cli+json |
| <a id="s-8c1286783e86"></a>`cli_commands` | ["collection upload discard"] |
| <a id="s-02c55e283179"></a>`client` | ApiClient |
| <a id="s-85310232f2aa"></a>`method` | POST |
| <a id="s-8612a8409d72"></a>`operation_id` | discard_collection_upload |
| <a id="s-e53be65d7bfe"></a>`path` | /v1/collection-upload-sessions/{collection_id}/discard |
| <a id="s-7d8dd17dd74f"></a>`provider_evidence` | None |
| <a id="s-0c63d41939fa"></a>`read_collection` | None |
| <a id="s-8f3ef3a69f4a"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/discard](../http/post-v1-collection-upload-sessions-collection-id-discard.md)
- [piggity collection upload discard](../../piggity/cli/piggity-collection-upload-discard.md)

## Governing policies

- <a id="pa-5768119072c7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-a4f56ba692ac"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-3f095ead9043"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/56`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 801ebb6126f094888c7f18d3b5a64afac285fd712928ec2ef6fee07f16dff411 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection upload discard"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "discard_collection_upload",
  "path": "/v1/collection-upload-sessions/{collection_id}/discard",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
