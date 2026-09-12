# Operation parity: head_collection_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-head-collection-provenance-journal:656f831a47 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-133bc9ef7ecc"></a>
| Concern | Contract |
|---|---|
| <a id="s-28d6d0a14a42"></a>`application` | riverhog |
| <a id="s-6510756a6d3a"></a>`classification` | standard-tool/protocol |
| <a id="s-c07653d563e3"></a>`cli_commands` | [] |
| <a id="s-33406812c041"></a>`client` | None |
| <a id="s-0db72ebd3c7c"></a>`method` | HEAD |
| <a id="s-8b62357c45e4"></a>`operation_id` | head_collection_provenance_journal |
| <a id="s-e20b6dfeecb6"></a>`path` | /v1/collections/{collection_id}/provenance/journals/{journal_id} |
| <a id="s-2dab6a3bf249"></a>`provider_evidence` | None |
| <a id="s-2766e43b5825"></a>`read_collection` | None |
| <a id="s-a83b3d291eec"></a>`response_authority` | stream-or-empty |

## Governing policies

- <a id="pa-79ac71d1252e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-6387c1d92992"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-62e5f2650e10"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/80`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2053e6c469b6a0ef8143dd5506bb3e747cca16c2cef5c28c1c751513b7edbb33 -->

```json
{
  "application": "riverhog",
  "classification": "standard-tool/protocol",
  "cli_commands": [],
  "client": null,
  "method": "HEAD",
  "operation_id": "head_collection_provenance_journal",
  "path": "/v1/collections/{collection_id}/provenance/journals/{journal_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "stream-or-empty"
}
```
