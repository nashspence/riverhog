# HEAD /v1/collections/{collection_id}/provenance/journals/{journal_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:head-v1-collections-collection-id-provena-240d79153d:2f6d1f48ae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-e23dde9f13"></a>
| Concern | Contract |
|---|---|
| <a id="s-127d3b9ffc"></a>`application` | riverhog |
| <a id="s-72283abfb6"></a>`classification` | standard-tool/protocol |
| <a id="s-e43aa3542e"></a>`method` | HEAD |
| <a id="s-b4d97430d8"></a>`operation_id` | head_collection_provenance_journal |
| <a id="s-ceac4a812e"></a>`path` | /v1/collections/{collection_id}/provenance/journals/{journal_id} |
| <a id="s-9634f16196"></a>`response_authority` | stream-or-empty |

## Governing policies

- <a id="pa-f96ebbef05"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "standard-tool/protocol",
  "cli_bindings": [],
  "cli_commands": [],
  "client": null,
  "client_bindings": [],
  "method": "HEAD",
  "operation_id": "head_collection_provenance_journal",
  "path": "/v1/collections/{collection_id}/provenance/journals/{journal_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "stream-or-empty"
}
```

</details>

### Machine authority

- `/external_contract/http_route_supplements/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 713dce9e045adad28974abdcb257225c8e66db3b5fbb08d25cdae63625667f26 -->

```json
{
  "application": "riverhog",
  "classification": "standard-tool/protocol",
  "method": "HEAD",
  "operation_id": "head_collection_provenance_journal",
  "path": "/v1/collections/{collection_id}/provenance/journals/{journal_id}",
  "response_authority": "stream-or-empty"
}
```
