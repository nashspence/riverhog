# Operation parity: list_admission_policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-list-admission-policies:1dff9fb756 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [admission-policies](families/admission-policies/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-162e80efa6"></a>
| Concern | Contract |
|---|---|
| <a id="s-18c98d72be"></a>`application` | stove0 |
| <a id="s-596d4dcea7"></a>`classification` | human-cli+json |
| <a id="s-7db3c38fa2"></a>`cli_commands` | ["admission policy list"] |
| <a id="s-1ab3eb9e63"></a>`client` | Stove0ApiClient |
| <a id="s-34bf885c32"></a>`method` | GET |
| <a id="s-9dd56ad2a6"></a>`operation_id` | list_admission_policies |
| <a id="s-d18b0eee67"></a>`path` | /v1/admission-policies |
| <a id="s-8d1d89da65"></a>`provider_evidence` | None |
| <a id="s-d9303005b8"></a>`read_collection` | None |
| <a id="s-6cf411ea13"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/admission-policies](../http/get-v1-admission-policies.md)
- [stove0-client admission policy list](../../stove0-client/cli/stove0-client-admission-policy-list.md)

## Governing policies

- <a id="pa-ab7837053a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-024dac9ce2"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-d4494e88dd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/118`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 910daa8a36880aaa82d162c004f0181b0255eb4789d22b238d9cf9b025a8ddcb -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "admission policy list"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "list_admission_policies",
  "path": "/v1/admission-policies",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
