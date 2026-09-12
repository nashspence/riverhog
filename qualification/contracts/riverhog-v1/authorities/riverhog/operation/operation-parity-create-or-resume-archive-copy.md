# Operation parity: create_or_resume_archive_copy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-or-resume-archive-copy:d6aeda2ae2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f40c99302785"></a>
| Concern | Contract |
|---|---|
| <a id="s-7233e445afd6"></a>`application` | riverhog |
| <a id="s-88b7dfd99da1"></a>`classification` | human-cli+json |
| <a id="s-67c7218976bc"></a>`cli_commands` | ["archive copy start"] |
| <a id="s-7bacf2c71d66"></a>`client` | ApiClient |
| <a id="s-b378d6cd098a"></a>`method` | POST |
| <a id="s-4743124f23c2"></a>`operation_id` | create_or_resume_archive_copy |
| <a id="s-df58aa087cc5"></a>`path` | /v1/archive/copies |
| <a id="s-0263b8e2aefa"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-c534d22d4dcb"></a>`read_collection` | None |
| <a id="s-10384230e7c2"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/archive/copies](../http/post-v1-archive-copies.md)
- [piggity archive copy start](../../piggity/cli/piggity-archive-copy-start.md)

## Governing policies

- <a id="pa-e867dfcafdeb"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-4331040e0261"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-3a6ba21fe2f5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/13`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e7c4963425ffef94099b0c90954ef0c94d99c9588d62a81f9311aa38b282afa -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive copy start"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_or_resume_archive_copy",
  "path": "/v1/archive/copies",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
