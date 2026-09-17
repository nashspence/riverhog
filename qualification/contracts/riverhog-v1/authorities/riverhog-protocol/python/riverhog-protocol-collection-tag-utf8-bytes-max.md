# riverhog_protocol.COLLECTION_TAG_UTF8_BYTES_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-tag-utf8-bytes-max:5a84d825e0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b46c45ffcd"></a>
- <a id="s-1981bda0a6"></a>`distribution`: `riverhog-protocol`
- <a id="s-e32868c1e9"></a>`module`: `riverhog_protocol`
- <a id="s-8cb12f5b06"></a>`name`: `COLLECTION_TAG_UTF8_BYTES_MAX`
- <a id="s-581952d139"></a>`unit`: `export`

### Declared structure

- <a id="s-e430f4fd22"></a>`kind`: `"constant"`
- <a id="s-e08e8e32e4"></a>`value`: `65536`

## Governing policies

- <a id="pa-1d175c671e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.COLLECTION_TAG_UTF8_BYTES_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 58244aeb561f83dec269f58b4b63c466083b39c0a7e6f4f6914627e1f911c7c3 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 65536
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "COLLECTION_TAG_UTF8_BYTES_MAX",
  "unit": "export"
}
```

</details>
