# Operation parity: ftp_adapter_health_live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog-ftp-adapter:operation-parity-ftp-adapter-health-live:a8c347c2e7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [operation](index.md) |
| Family | [health](index.md#f-fdb2955f62e7) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f581b980fae0"></a>
| Concern | Contract |
|---|---|
| <a id="s-b9af6e817dbe"></a>`application` | riverhog-ftp-adapter |
| <a id="s-dd7a49055244"></a>`classification` | standard-tool/protocol |
| <a id="s-42880244243c"></a>`cli_commands` | [] |
| <a id="s-feab818acf9b"></a>`client` | RiverhogFtpAdapterClient |
| <a id="s-a96413b9aad5"></a>`method` | GET |
| <a id="s-7a048500b418"></a>`operation_id` | ftp_adapter_health_live |
| <a id="s-fa6230b405a3"></a>`path` | /health/live |
| <a id="s-2d405dce6ee5"></a>`provider_evidence` | None |
| <a id="s-fed443030580"></a>`read_collection` | None |
| <a id="s-dbe00d07e3a1"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /health/live](../http/get-health-live.md)

## Governing policies

- <a id="pa-8bbfb9a53334"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-8331bc21a54d"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-b566293a6f71"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/109`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7e3a9c1399bd595e3430922247dd8abe6605cf1528d5eb1086acaca7952bb9b0 -->

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "standard-tool/protocol",
  "cli_commands": [],
  "client": "RiverhogFtpAdapterClient",
  "method": "GET",
  "operation_id": "ftp_adapter_health_live",
  "path": "/health/live",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
