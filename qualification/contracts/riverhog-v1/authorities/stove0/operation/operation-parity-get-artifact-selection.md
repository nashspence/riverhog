# Operation parity: get_artifact_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-artifact-selection:cde72fc375 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [artifact-selections](families/artifact-selections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-31bba760dce9"></a>
| Concern | Contract |
|---|---|
| <a id="s-759a24cb0390"></a>`application` | stove0 |
| <a id="s-4773d3ee72ef"></a>`classification` | human-cli+json |
| <a id="s-b1bd00c64e59"></a>`cli_commands` | ["selection show"] |
| <a id="s-8c15f17038db"></a>`client` | Stove0ApiClient |
| <a id="s-67562379ae68"></a>`method` | GET |
| <a id="s-16f4d237c96a"></a>`operation_id` | get_artifact_selection |
| <a id="s-d911d790f627"></a>`path` | /v1/artifact-selections/{selection_sha256} |
| <a id="s-a949dc3b7278"></a>`provider_evidence` | None |
| <a id="s-f51c521722d1"></a>`read_collection` | {"authority": "artifact-selection", "authority_parameter": "selection_sha256", "cursor_parameter": "continuation", "fixed_limit": 256, "kind": "exact-authority-page"} |
| <a id="s-ee2ed35884cd"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/artifact-selections/{selection_sha256}](../http/get-v1-artifact-selections-selection-sha256.md)
- [stove0 selection show](../cli/stove0-selection-show.md)

## Governing policies

- <a id="pa-dbaa3ae33ece"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-db68ade1fa10"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-ce237d5d5162"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/123`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fd9fa51297007777050cab06b4db0978155bc657f20a0f2047a36fe8f63f863 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "selection show"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "get_artifact_selection",
  "path": "/v1/artifact-selections/{selection_sha256}",
  "provider_evidence": null,
  "read_collection": {
    "authority": "artifact-selection",
    "authority_parameter": "selection_sha256",
    "cursor_parameter": "continuation",
    "fixed_limit": 256,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```
