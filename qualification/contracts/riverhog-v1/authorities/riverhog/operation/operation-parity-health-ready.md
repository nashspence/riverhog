# Operation parity: health_ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-health-ready:fdca1976b1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [health](families/health/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3374a61243"></a>
| Concern | Contract |
|---|---|
| <a id="s-a986e9ca86"></a>`application` | riverhog |
| <a id="s-ed2effe342"></a>`classification` | standard-tool/protocol |
| <a id="s-fc4872537e"></a>`cli_commands` | [] |
| <a id="s-31cb6cd86b"></a>`client` | None |
| <a id="s-e031025223"></a>`method` | GET |
| <a id="s-5381134f9b"></a>`operation_id` | health_ready |
| <a id="s-2d2b7915ba"></a>`path` | /health/ready |
| <a id="s-ac40e58fa7"></a>`provider_evidence` | None |
| <a id="s-ae46454255"></a>`read_collection` | None |
| <a id="s-9f9bf5e14f"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /health/ready](../http/get-health-ready.md)

## Governing policies

- <a id="pa-61e3bdac95"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-e9901ea6d2"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-e48c922919"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7dfaac98b8b1d5d5cf803515b95c18d10d3812eaf49414299ccf2217fec48daa -->

```json
{
  "application": "riverhog",
  "classification": "standard-tool/protocol",
  "cli_commands": [],
  "client": null,
  "method": "GET",
  "operation_id": "health_ready",
  "path": "/health/ready",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
