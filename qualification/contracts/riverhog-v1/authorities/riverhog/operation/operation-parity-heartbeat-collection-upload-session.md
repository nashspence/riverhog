# Operation parity: heartbeat_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-heartbeat-collection-upload-session:e7e22bf502 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6a36c13f38"></a>
| Concern | Contract |
|---|---|
| <a id="s-36d4b99947"></a>`application` | riverhog |
| <a id="s-4405710f12"></a>`classification` | client-only-primitive |
| <a id="s-57ee893430"></a>`cli_commands` | [] |
| <a id="s-4cf367809e"></a>`client` | ApiClient |
| <a id="s-592c75e0bd"></a>`method` | POST |
| <a id="s-120f76f167"></a>`operation_id` | heartbeat_collection_upload_session |
| <a id="s-2494d09055"></a>`path` | /v1/collection-upload-sessions/{collection_id}/heartbeat |
| <a id="s-b092885777"></a>`provider_evidence` | None |
| <a id="s-7a3656dc76"></a>`read_collection` | None |
| <a id="s-cfef3f3299"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/heartbeat](../http/post-v1-collection-upload-sessions-collection-id-heartbeat.md)

## Governing policies

- <a id="pa-73119b6f50"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-121d9d52fe"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-f07f8df68e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/60`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75fa3a256212f4c3d9750f7453f35ca69678512e45ba366ce2f31491dd1b30fd -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "heartbeat_collection_upload_session",
  "path": "/v1/collection-upload-sessions/{collection_id}/heartbeat",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
