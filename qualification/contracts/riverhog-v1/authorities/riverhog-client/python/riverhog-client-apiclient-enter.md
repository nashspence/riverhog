# riverhog_client.ApiClient.__enter__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-enter:8023d37ce0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a3f159e2b4"></a>
- <a id="s-54f03f50dc"></a>`distribution`: `riverhog-client`
- <a id="s-b132aa9cb6"></a>`module`: `riverhog_client`
- <a id="s-96a4d26867"></a>`name`: `__enter__`
- <a id="s-2ac38ab6fe"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-9f3d378f94"></a>`unit`: `member`

### Declared structure

- <a id="s-31a2c25a65"></a>`kind`: `"method"`
- <a id="s-8a2f4056d8"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-53d91a28fc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.__enter__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72e7748f825c546dd1b1c09f49f2c68f80ba9b1b2f1957877a33bb1b44c9776f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "__enter__",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
