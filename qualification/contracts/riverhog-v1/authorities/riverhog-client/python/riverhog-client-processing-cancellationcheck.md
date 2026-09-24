# riverhog_client.processing.CancellationCheck

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-cancellationcheck:3a54c075de -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d766b6445a"></a>
- <a id="s-30add7651b"></a>`distribution`: `riverhog-client`
- <a id="s-55122aa728"></a>`module`: `riverhog_client.processing`
- <a id="s-cf86ebd58e"></a>`name`: `CancellationCheck`
- <a id="s-3879c80c1e"></a>`unit`: `export`

### Declared structure

- <a id="s-96538abf0b"></a>`kind`: `"object"`
- <a id="s-45017f2dab"></a>`type`: `"collections.abc._CallableGenericAlias"`

## Governing policies

- <a id="pa-0ed39057b9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CancellationCheck`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0198952f83890ff4ea4dbc2efecb957155371f19590cba3dbe6d5b85c245e146 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "collections.abc._CallableGenericAlias"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "CancellationCheck",
  "unit": "export"
}
```

</details>
