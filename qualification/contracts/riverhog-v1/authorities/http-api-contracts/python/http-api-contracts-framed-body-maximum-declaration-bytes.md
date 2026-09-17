# http_api_contracts.FRAMED_BODY_MAXIMUM_DECLARATION_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-framed-body-maximum-de-bbd36b39ef:54b5c197f4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-64969c89a6"></a>
- <a id="s-676f28b350"></a>`distribution`: `http-api-contracts`
- <a id="s-185a7705d7"></a>`module`: `http_api_contracts`
- <a id="s-6957f89ae5"></a>`name`: `FRAMED_BODY_MAXIMUM_DECLARATION_BYTES`
- <a id="s-cab012fb47"></a>`unit`: `export`

### Declared structure

- <a id="s-84ec6513c8"></a>`kind`: `"constant"`
- <a id="s-49e6dd61ff"></a>`value`: `32768`

## Governing policies

- <a id="pa-1553839c26"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.FRAMED_BODY_MAXIMUM_DECLARATION_BYTES`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7382824dec582c6020f3a3b418a0fa6da413de68c4538b647d7cc0ea4fbba9a2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 32768
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "FRAMED_BODY_MAXIMUM_DECLARATION_BYTES",
  "unit": "export"
}
```

</details>
