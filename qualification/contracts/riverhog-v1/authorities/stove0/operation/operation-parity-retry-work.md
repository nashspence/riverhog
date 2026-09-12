# Operation parity: retry_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-retry-work:aaa9396625 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [work](families/work/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d08d980d086d"></a>
| Concern | Contract |
|---|---|
| <a id="s-59300ef7343b"></a>`application` | stove0 |
| <a id="s-e4b9cd1e9933"></a>`classification` | human-cli+json |
| <a id="s-45670caf8bc5"></a>`cli_commands` | ["work retry"] |
| <a id="s-d3d0f02ec26b"></a>`client` | Stove0ApiClient |
| <a id="s-e7900379ed3d"></a>`method` | POST |
| <a id="s-36117474dec7"></a>`operation_id` | retry_work |
| <a id="s-0001ddf95b92"></a>`path` | /v1/work/{work_id}/retry |
| <a id="s-7331ecbfae80"></a>`provider_evidence` | None |
| <a id="s-40824fa4a536"></a>`read_collection` | None |
| <a id="s-b280c784545b"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/work/{work_id}/retry](../http/post-v1-work-work-id-retry.md)
- [stove0 work retry](../cli/stove0-work-retry.md)

## Governing policies

- <a id="pa-209015975b30"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-ede2b6a17849"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-b4ce57dcdf26"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/144`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c9475cadb8994944c85f857f04c2120b3eb6117ac13e6c2172e62a680825ff5 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work retry"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "retry_work",
  "path": "/v1/work/{work_id}/retry",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
