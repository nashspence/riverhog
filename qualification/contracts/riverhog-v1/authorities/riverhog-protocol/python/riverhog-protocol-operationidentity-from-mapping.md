# riverhog_protocol.OperationIdentity.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-operationidentity-from-mapping:b0c25eb9d6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ef5adc4e2c"></a>
- <a id="s-4de7e85510"></a>`distribution`: `riverhog-protocol`
- <a id="s-652cb3bd67"></a>`module`: `riverhog_protocol`
- <a id="s-e24267f133"></a>`name`: `from_mapping`
- <a id="s-eefd925b74"></a>`owner`: `riverhog_protocol.OperationIdentity`
- <a id="s-cccbcb188f"></a>`unit`: `member`

### Declared structure

- <a id="s-e7c8425688"></a>`kind`: `"classmethod"`
- <a id="s-ef6a8c7598"></a>`signature`: `"\"(cls, value: 'Mapping[str, object]') -> 'OperationIdentity'\""`

## Maintained corroboration

### Related interface records

- [OperationIdentity](riverhog-protocol-operationidentity.md)

## Governing policies

- <a id="pa-d88981e1be"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.OperationIdentity.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a89d875877c58f77e6d8e965a557b692301b017c55b1727d66da9b2a36f64d0c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Mapping[str, object]') -> 'OperationIdentity'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "from_mapping",
  "owner": "riverhog_protocol.OperationIdentity",
  "unit": "member"
}
```

</details>
