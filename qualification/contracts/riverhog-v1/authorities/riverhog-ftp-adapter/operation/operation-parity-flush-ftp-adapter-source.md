# Operation parity: flush_ftp_adapter_source

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog-ftp-adapter:operation-parity-flush-ftp-adapter-source:aa0dc84b87 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [operation](index.md) |
| Family | [sources](index.md#f-c980634fc3d2) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3a7b1e557c36"></a>
| Concern | Contract |
|---|---|
| <a id="s-55ae8975f364"></a>`application` | riverhog-ftp-adapter |
| <a id="s-afc670bde370"></a>`classification` | human-cli+json |
| <a id="s-2f182d814fed"></a>`cli_commands` | ["flush"] |
| <a id="s-f0efa1b4456e"></a>`client` | RiverhogFtpAdapterClient |
| <a id="s-7bb6635761d4"></a>`method` | POST |
| <a id="s-fbbea80f3651"></a>`operation_id` | flush_ftp_adapter_source |
| <a id="s-e70a47d1d8a3"></a>`path` | /v1/sources/{source_id}/flush |
| <a id="s-598159d3b8bf"></a>`provider_evidence` | None |
| <a id="s-7eab2d60cdb5"></a>`read_collection` | None |
| <a id="s-8a4d54d56499"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/sources/{source_id}/flush](../http/post-v1-sources-source-id-flush.md)

## Governing policies

- <a id="pa-89221cba476f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-512a6505f733"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-0c1caa984ee3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/112`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df9f25bf18577e80f9f417ed9806ca4716395dd67a2028700fd2a7be09f6be99 -->

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "human-cli+json",
  "cli_commands": [
    "flush"
  ],
  "client": "RiverhogFtpAdapterClient",
  "method": "POST",
  "operation_id": "flush_ftp_adapter_source",
  "path": "/v1/sources/{source_id}/flush",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
