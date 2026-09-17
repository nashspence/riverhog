# riverhog_client.ProducerInput

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-producerinput:45e9fd91d2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a0df3e2b48"></a>
- <a id="s-100da8ef68"></a>`distribution`: `riverhog-client`
- <a id="s-4cf98fa0a2"></a>`module`: `riverhog_client`
- <a id="s-26969ade16"></a>`name`: `ProducerInput`
- <a id="s-be6e67d4ac"></a>`unit`: `export`

### Declared structure

- <a id="s-2d0d93d389"></a>`kind`: `"object"`
- <a id="s-7fa9ff03d7"></a>`type`: `"types.UnionType"`

## Governing policies

- <a id="pa-aa34297d1a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.ProducerInput`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b92448febedd32892fb3ff727bfe267b21b934406640450cfc2a89add5b636b -->

```json
{
  "contract": {
    "kind": "object",
    "type": "types.UnionType"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ProducerInput",
  "unit": "export"
}
```

</details>
