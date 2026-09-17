# http_api_contracts.Sha256Identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-sha256identity:e8c39d7edf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-18a154f887"></a>
- <a id="s-8ea6fd5698"></a>`distribution`: `http-api-contracts`
- <a id="s-36c4ccce0d"></a>`module`: `http_api_contracts`
- <a id="s-bcd0628314"></a>`name`: `Sha256Identity`
- <a id="s-ee84ff34c1"></a>`unit`: `export`

### Declared structure

- <a id="s-0c5067ef04"></a>`kind`: `"object"`
- <a id="s-cd6f059e4e"></a>`type`: `"typing._AnnotatedAlias"`

## Governing policies

- <a id="pa-9b386e4804"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.Sha256Identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 016df0da975b684e5d64b86c01a237843f2de186f5f7077b788e14b5da404780 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "Sha256Identity",
  "unit": "export"
}
```

</details>
