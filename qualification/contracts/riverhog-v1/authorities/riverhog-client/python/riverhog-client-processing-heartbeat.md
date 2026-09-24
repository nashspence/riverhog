# riverhog_client.processing.Heartbeat

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-heartbeat:a19a21e3d2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf26c63285"></a>
- <a id="s-fe2c4d1354"></a>`distribution`: `riverhog-client`
- <a id="s-8b5837f564"></a>`module`: `riverhog_client.processing`
- <a id="s-430b89976b"></a>`name`: `Heartbeat`
- <a id="s-36926dde9e"></a>`unit`: `export`

### Declared structure

- <a id="s-fdb6f1a990"></a>`kind`: `"object"`
- <a id="s-c18495c375"></a>`type`: `"collections.abc._CallableGenericAlias"`

## Governing policies

- <a id="pa-277d440c30"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.Heartbeat`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae95d795f1e5a26894005e17161404d2179df8e61035effe3ddc72e73551da88 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "collections.abc._CallableGenericAlias"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "Heartbeat",
  "unit": "export"
}
```

</details>
