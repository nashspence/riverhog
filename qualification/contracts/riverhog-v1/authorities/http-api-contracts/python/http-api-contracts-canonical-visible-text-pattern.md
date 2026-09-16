# http_api_contracts.CANONICAL_VISIBLE_TEXT_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-canonical-visible-text-pattern:73875d3e54 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fd478e12ff"></a>
- <a id="s-712a1af91b"></a>`distribution`: `http-api-contracts`
- <a id="s-be762333dd"></a>`module`: `http_api_contracts`
- <a id="s-dcb71574a9"></a>`name`: `CANONICAL_VISIBLE_TEXT_PATTERN`
- <a id="s-08970d3935"></a>`unit`: `export`

### Declared structure

- <a id="s-4f84ad5ce8"></a>`kind`: `"constant"`
- <a id="s-db0f46726c"></a>`value`: `"^\\S(?:[\\s\\S]*\\S)?$"`

## Governing policies

- <a id="pa-7857094648"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.CANONICAL_VISIBLE_TEXT_PATTERN`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce33067b547e7702e3ba3b9a9d4496156eac145d35d62bb05d25006da2e4db34 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^\\S(?:[\\s\\S]*\\S)?$"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "CANONICAL_VISIBLE_TEXT_PATTERN",
  "unit": "export"
}
```

</details>
