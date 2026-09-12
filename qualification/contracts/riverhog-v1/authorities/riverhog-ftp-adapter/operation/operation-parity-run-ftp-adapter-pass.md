# Operation parity: run_ftp_adapter_pass

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog-ftp-adapter:operation-parity-run-ftp-adapter-pass:51f48df72d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [operation](index.md) |
| Family | [run](index.md#f-8abb6fb27d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-36570ed92e"></a>
| Concern | Contract |
|---|---|
| <a id="s-a73ed4c5d5"></a>`application` | riverhog-ftp-adapter |
| <a id="s-517a2ff442"></a>`classification` | human-cli+json |
| <a id="s-7b20ddb53a"></a>`cli_commands` | ["run"] |
| <a id="s-2903e4fc76"></a>`client` | RiverhogFtpAdapterClient |
| <a id="s-b013791049"></a>`method` | POST |
| <a id="s-816311ec06"></a>`operation_id` | run_ftp_adapter_pass |
| <a id="s-f7ab3e8618"></a>`path` | /v1/run |
| <a id="s-8c79ebcbe8"></a>`provider_evidence` | None |
| <a id="s-8c73f763a8"></a>`read_collection` | None |
| <a id="s-d1fbb4ca96"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/run](../http/post-v1-run.md)

## Governing policies

- <a id="pa-504ae2ec93"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5c83dd2929"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-b4df65eecf"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/111`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed458d5ef46788b6f27bc9567d45adb4de10dac8dbea5a300b70d0f9a5f4d0d9 -->

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "human-cli+json",
  "cli_commands": [
    "run"
  ],
  "client": "RiverhogFtpAdapterClient",
  "method": "POST",
  "operation_id": "run_ftp_adapter_pass",
  "path": "/v1/run",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
