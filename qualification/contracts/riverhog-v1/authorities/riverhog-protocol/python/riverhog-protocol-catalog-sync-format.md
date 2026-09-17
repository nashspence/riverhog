# riverhog_protocol.CATALOG_SYNC_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalog-sync-format:69e310d3d9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7c33fd62a"></a>
- <a id="s-2f5e847b53"></a>`distribution`: `riverhog-protocol`
- <a id="s-330a9a9e00"></a>`module`: `riverhog_protocol`
- <a id="s-735eb0294c"></a>`name`: `CATALOG_SYNC_FORMAT`
- <a id="s-c866478459"></a>`unit`: `export`

### Declared structure

- <a id="s-0a74df6af2"></a>`kind`: `"constant"`
- <a id="s-c550040122"></a>`value`: `"riverhog-catalog-sync/v1"`

## Governing policies

- <a id="pa-13440a0aed"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CATALOG_SYNC_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3cf66083a09a42ff1eadab6f53bb9f544871cd8fc8edf72a829090f4d99b18a1 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-catalog-sync/v1"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CATALOG_SYNC_FORMAT",
  "unit": "export"
}
```

</details>
