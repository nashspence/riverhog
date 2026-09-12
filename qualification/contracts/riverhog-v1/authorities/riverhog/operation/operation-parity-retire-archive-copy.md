# Operation parity: retire_archive_copy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-retire-archive-copy:83a54b3e0b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d717a4e09c"></a>
| Concern | Contract |
|---|---|
| <a id="s-beecf09cc1"></a>`application` | riverhog |
| <a id="s-b319fc4bb1"></a>`classification` | human-cli+json |
| <a id="s-e39e238eb3"></a>`cli_commands` | ["archive retire"] |
| <a id="s-0b3acd7365"></a>`client` | ApiClient |
| <a id="s-60dd310199"></a>`method` | POST |
| <a id="s-52e20f20bd"></a>`operation_id` | retire_archive_copy |
| <a id="s-719bce587e"></a>`path` | /v1/archive/copies/retire |
| <a id="s-98d7bcf940"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-b6e88a6ae1"></a>`read_collection` | None |
| <a id="s-dbae5978d3"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/archive/copies/retire](../http/post-v1-archive-copies-retire.md)
- [piggity archive retire](../../piggity/cli/piggity-archive-retire.md)

## Governing policies

- <a id="pa-fbb9e2c982"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-0a57b46a9f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-dfc4b8b11e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/14`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d44330ae27027863e122abb5d442c3d5895aca64a4696b9dadf3ba9eaf6c4b8 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive retire"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "retire_archive_copy",
  "path": "/v1/archive/copies/retire",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
