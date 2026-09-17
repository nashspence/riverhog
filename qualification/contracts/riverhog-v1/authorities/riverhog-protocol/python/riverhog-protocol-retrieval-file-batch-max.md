# riverhog_protocol.RETRIEVAL_FILE_BATCH_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrieval-file-batch-max:be1e891cb4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-09183bb334"></a>
- <a id="s-e339f91e8a"></a>`distribution`: `riverhog-protocol`
- <a id="s-838748f981"></a>`module`: `riverhog_protocol`
- <a id="s-147270907e"></a>`name`: `RETRIEVAL_FILE_BATCH_MAX`
- <a id="s-1185e9105c"></a>`unit`: `export`

### Declared structure

- <a id="s-1cf6bd2954"></a>`kind`: `"constant"`
- <a id="s-218f8b2758"></a>`value`: `10000`

## Governing policies

- <a id="pa-8fb25d8019"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RETRIEVAL_FILE_BATCH_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e122d55b716f676e1b157d28d38c1cf0cf985e730968e18d2fd1e235d3e3c18 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 10000
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RETRIEVAL_FILE_BATCH_MAX",
  "unit": "export"
}
```

</details>
