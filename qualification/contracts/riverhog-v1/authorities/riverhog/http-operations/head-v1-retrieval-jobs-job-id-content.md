# HEAD /v1/retrieval-jobs/{job_id}/content

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:head-v1-retrieval-jobs-job-id-content:c99110dae4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-c7ce8a7896"></a>
| Concern | Contract |
|---|---|
| <a id="s-9489d667cb"></a>`application` | `"riverhog"` |
| <a id="s-d2f6cb9473"></a>`classification` | `"standard-tool/protocol"` |
| <a id="s-038a94b1d0"></a>`method` | `"HEAD"` |
| <a id="s-f72082f88f"></a>`operation_id` | `"head_retrieval_file"` |
| <a id="s-3260b16d53"></a>`path` | `"/v1/retrieval-jobs/{job_id}/content"` |
| <a id="s-4a64c9ee67"></a>`response_authority` | `"stream-or-empty"` |

## Governing policies

- <a id="pa-8c49065d35"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)

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
  "operation_id": "head_retrieval_file",
  "path": "/v1/retrieval-jobs/{job_id}/content",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "stream-or-empty"
}
```

</details>

### Machine authority

- `/external_contract/http_route_supplements/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f2bebfe422c86b81014093cfe92b4a9668085111c86edae504b077fd236bbb4a -->

```json
{
  "application": "riverhog",
  "classification": "standard-tool/protocol",
  "method": "HEAD",
  "operation_id": "head_retrieval_file",
  "path": "/v1/retrieval-jobs/{job_id}/content",
  "response_authority": "stream-or-empty"
}
```

</details>
