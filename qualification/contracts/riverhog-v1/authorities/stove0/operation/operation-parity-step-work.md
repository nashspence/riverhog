# Operation parity: step_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-step-work:9bce803a3e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [work](families/work/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-2b7ad966a40d"></a>
| Concern | Contract |
|---|---|
| <a id="s-ddc0876abad7"></a>`application` | stove0 |
| <a id="s-f418f0c9c6ad"></a>`classification` | human-cli+json |
| <a id="s-443d9b1618a4"></a>`cli_commands` | ["work step"] |
| <a id="s-0d63459546cf"></a>`client` | Stove0ApiClient |
| <a id="s-6d415d31debc"></a>`method` | POST |
| <a id="s-1c0df48484b8"></a>`operation_id` | step_work |
| <a id="s-be105a16d404"></a>`path` | /v1/work/{work_id}/step |
| <a id="s-60c8d827363c"></a>`provider_evidence` | None |
| <a id="s-827216163f5d"></a>`read_collection` | None |
| <a id="s-ad9db4ac73d7"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/work/{work_id}/step](../http/post-v1-work-work-id-step.md)
- [stove0 work step](../cli/stove0-work-step.md)

## Governing policies

- <a id="pa-a87899a2fff9"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-5961590580a8"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-6e8a0bd693b6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/145`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 45be03422b3371bbe34dd93762c950bdd8d1c8df72074a0c5b002015b989d586 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work step"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "step_work",
  "path": "/v1/work/{work_id}/step",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
