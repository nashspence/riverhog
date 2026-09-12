# Operation parity: create_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-create-work:736597124e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [work](families/work/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c34b11eeeb4f"></a>
| Concern | Contract |
|---|---|
| <a id="s-4d4b08c9b20f"></a>`application` | stove0 |
| <a id="s-092b9d471a7c"></a>`classification` | human-cli+json |
| <a id="s-65f47c140849"></a>`cli_commands` | ["work create"] |
| <a id="s-c9ce7e6f0cff"></a>`client` | Stove0ApiClient |
| <a id="s-83ab60d39112"></a>`method` | POST |
| <a id="s-a33c27cdb8f2"></a>`operation_id` | create_work |
| <a id="s-40cc02b651d3"></a>`path` | /v1/work |
| <a id="s-593db5e9a6ad"></a>`provider_evidence` | None |
| <a id="s-677dab351615"></a>`read_collection` | None |
| <a id="s-651947c66351"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/work](../http/post-v1-work.md)
- [stove0 work create](../cli/stove0-work-create.md)

## Governing policies

- <a id="pa-444cd36d453c"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-1417fa8dd3ea"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-69b32e5d4c81"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/140`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7becd13088668454120980b3a36b5e94e0abfd52f4911c50a123bc595b8a1518 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work create"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "create_work",
  "path": "/v1/work",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
