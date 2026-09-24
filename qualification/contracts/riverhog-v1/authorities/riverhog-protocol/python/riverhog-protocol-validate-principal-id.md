# riverhog_protocol.validate_principal_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-validate-principal-id:a3385cfc6c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-12ec12a406"></a>
- <a id="s-75825e8b5d"></a>`distribution`: `riverhog-protocol`
- <a id="s-c904e9d1ad"></a>`module`: `riverhog_protocol`
- <a id="s-a9850f7bb0"></a>`name`: `validate_principal_id`
- <a id="s-8b7d2e776d"></a>`unit`: `export`

### Declared structure

- <a id="s-85601ae821"></a>`kind`: `"function"`
- <a id="s-b39c22a56c"></a>`signature`: `"\"(value: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-5a7acbccd0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.validate_principal_id`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 16fd7a673d8d64d3169bdf450e19eed000c4b2210b3715415909dc17413b6a16 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_principal_id",
  "unit": "export"
}
```

</details>
