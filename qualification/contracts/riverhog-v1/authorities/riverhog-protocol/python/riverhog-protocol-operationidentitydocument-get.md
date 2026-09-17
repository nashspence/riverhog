# riverhog_protocol.OperationIdentityDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-operationidentitydocument-get:62fa52bde3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2ef3b38f49"></a>
- <a id="s-8ad9b19cda"></a>`distribution`: `riverhog-protocol`
- <a id="s-b1b8c14cf0"></a>`module`: `riverhog_protocol`
- <a id="s-76d4255fe0"></a>`name`: `get`
- <a id="s-dcc263fb47"></a>`owner`: `riverhog_protocol.OperationIdentityDocument`
- <a id="s-da221d3476"></a>`unit`: `member`

### Declared structure

- <a id="s-32bd207e80"></a>`kind`: `"method"`
- <a id="s-f0494fd1cb"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [OperationIdentityDocument](riverhog-protocol-operationidentitydocument.md)

## Governing policies

- <a id="pa-e886b2439e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.OperationIdentityDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9eddcb6eb6942be33927f2737d0857687b750f4f0e45ef1a10c363364dcb2ee4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.OperationIdentityDocument",
  "unit": "member"
}
```

</details>
