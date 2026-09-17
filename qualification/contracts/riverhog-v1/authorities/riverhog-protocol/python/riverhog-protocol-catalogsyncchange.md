# riverhog_protocol.CatalogSyncChange

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncchange:268324e631 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-87903cd901"></a>
- <a id="s-f4f7e36bd4"></a>`distribution`: `riverhog-protocol`
- <a id="s-9d602ec53f"></a>`module`: `riverhog_protocol`
- <a id="s-34e7d26443"></a>`name`: `CatalogSyncChange`
- <a id="s-261fcb2f21"></a>`unit`: `export`

### Declared structure

- <a id="s-eeccc10643"></a>`kind`: `"object"`
- <a id="s-6d40cff36b"></a>`type`: `"typing._AnnotatedAlias"`

## Governing policies

- <a id="pa-0a6234d998"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncChange`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d95dc0d27dfbd0953d12e8209299f19e86fae40f080c476c346b0df23cee9bda -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncChange",
  "unit": "export"
}
```

</details>
