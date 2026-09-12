# Operation parity: head_retrieval_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-head-retrieval-file:62bdd60426 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-jobs](families/retrieval-jobs/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e932b9b08033"></a>
| Concern | Contract |
|---|---|
| <a id="s-bb898e8b5df2"></a>`application` | riverhog |
| <a id="s-a5276efad2b4"></a>`classification` | standard-tool/protocol |
| <a id="s-f080a0b5c8a6"></a>`cli_commands` | [] |
| <a id="s-f873e04e35b5"></a>`client` | None |
| <a id="s-df2703cb8c23"></a>`method` | HEAD |
| <a id="s-4dd708cdbae9"></a>`operation_id` | head_retrieval_file |
| <a id="s-e8cdbc61384d"></a>`path` | /v1/retrieval-jobs/{job_id}/content |
| <a id="s-bd90197338fb"></a>`provider_evidence` | None |
| <a id="s-108cc7663a3b"></a>`read_collection` | None |
| <a id="s-9ebdc237ecb3"></a>`response_authority` | stream-or-empty |

## Governing policies

- <a id="pa-1d9509185576"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-8b94b22ac502"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-8cf038003010"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/101`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c7fee6f5cae24b3cea55a8bd94a26766eb4ec068b682579b1a8adeca299b4d29 -->

```json
{
  "application": "riverhog",
  "classification": "standard-tool/protocol",
  "cli_commands": [],
  "client": null,
  "method": "HEAD",
  "operation_id": "head_retrieval_file",
  "path": "/v1/retrieval-jobs/{job_id}/content",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "stream-or-empty"
}
```
