# Operation parity: list_admissions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-list-admissions:c7a84bd6e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [admissions](families/admissions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-360fbc014451"></a>
| Concern | Contract |
|---|---|
| <a id="s-9a551558a9d7"></a>`application` | stove0 |
| <a id="s-0582a7f6aa0c"></a>`classification` | human-cli+json |
| <a id="s-916db46ec044"></a>`cli_commands` | ["admission list"] |
| <a id="s-32105071b49a"></a>`client` | Stove0ApiClient |
| <a id="s-d3a733419d58"></a>`method` | GET |
| <a id="s-8f2d48f77cfb"></a>`operation_id` | list_admissions |
| <a id="s-5b9d4b0720ad"></a>`path` | /v1/admissions |
| <a id="s-a2a12e388c54"></a>`provider_evidence` | None |
| <a id="s-b904090e0797"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-f715d88930e1"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/admissions](../http/get-v1-admissions.md)
- [stove0 admission list](../cli/stove0-admission-list.md)

## Governing policies

- <a id="pa-d6c220faf504"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-ae2ef574ff4e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-095ade99f188"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/121`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13952f53937a737508eaf4c0a85dbd5cce1d5a3ff997fb2b939cf54fc22b898b -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "admission list"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "list_admissions",
  "path": "/v1/admissions",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "operator-projection"
}
```
