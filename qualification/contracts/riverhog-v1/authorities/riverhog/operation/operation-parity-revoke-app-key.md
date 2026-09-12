# Operation parity: revoke_app_key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-revoke-app-key:9d2de65ee0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [apps](families/apps/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0c181652957c"></a>
| Concern | Contract |
|---|---|
| <a id="s-ddf35c3d4f20"></a>`application` | riverhog |
| <a id="s-5beec1ff741d"></a>`classification` | human-cli+json |
| <a id="s-900f34cb5310"></a>`cli_commands` | ["app key revoke"] |
| <a id="s-58ca9884ff3a"></a>`client` | ApiClient |
| <a id="s-27ab43a04278"></a>`method` | POST |
| <a id="s-29b2bfce257b"></a>`operation_id` | revoke_app_key |
| <a id="s-7cf715c75125"></a>`path` | /v1/apps/{app}/keys/{key_id}/revoke |
| <a id="s-b764040477c1"></a>`provider_evidence` | None |
| <a id="s-ce1f7caac93a"></a>`read_collection` | None |
| <a id="s-cb347865e3a5"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/revoke](../http/post-v1-apps-app-keys-key-id-revoke.md)
- [piggity app key revoke](../../piggity/cli/piggity-app-key-revoke.md)

## Governing policies

- <a id="pa-69646c054dca"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-f23c14e64ee8"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-ca07eeae6c20"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/10`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c03a8a21f91f1a7f6444dfa8dc3bffcec1a04e13fd32ae055b912ebc48696cc0 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key revoke"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "revoke_app_key",
  "path": "/v1/apps/{app}/keys/{key_id}/revoke",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
