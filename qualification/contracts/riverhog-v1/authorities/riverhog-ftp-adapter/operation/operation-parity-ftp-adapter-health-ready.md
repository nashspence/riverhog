# Operation parity: ftp_adapter_health_ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog-ftp-adapter:operation-parity-ftp-adapter-health-ready:2f9fc73012 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [operation](index.md) |
| Family | [health](index.md#f-fdb2955f62e7) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-87cd999ade99"></a>
| Concern | Contract |
|---|---|
| <a id="s-abf20f28a975"></a>`application` | riverhog-ftp-adapter |
| <a id="s-f0d954dfa28f"></a>`classification` | standard-tool/protocol |
| <a id="s-fdb4a59e65ee"></a>`cli_commands` | [] |
| <a id="s-040efe60c83f"></a>`client` | RiverhogFtpAdapterClient |
| <a id="s-395c5754e811"></a>`method` | GET |
| <a id="s-beee39c53edd"></a>`operation_id` | ftp_adapter_health_ready |
| <a id="s-1a93147d0e03"></a>`path` | /health/ready |
| <a id="s-6c9c117d180f"></a>`provider_evidence` | None |
| <a id="s-d45e2c6549fc"></a>`read_collection` | None |
| <a id="s-8392ee6f4eb3"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /health/ready](../http/get-health-ready.md)

## Governing policies

- <a id="pa-99d3cacc47a7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-a4377c7364c7"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-b77d10ac3e75"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/110`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f7a2208a1125494f199c490a54dc5b49f39bd920d0de5d97b8bedd86ec4fb87 -->

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "standard-tool/protocol",
  "cli_commands": [],
  "client": "RiverhogFtpAdapterClient",
  "method": "GET",
  "operation_id": "ftp_adapter_health_ready",
  "path": "/health/ready",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
