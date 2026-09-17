# riverhog_protocol.CATALOG_SYNC_PAGE_SIZE_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalog-sync-page-size-max:5717035523 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2f41bb89cb"></a>
- <a id="s-8e40df5e43"></a>`distribution`: `riverhog-protocol`
- <a id="s-75b3917656"></a>`module`: `riverhog_protocol`
- <a id="s-7b0a7c64b1"></a>`name`: `CATALOG_SYNC_PAGE_SIZE_MAX`
- <a id="s-27ee5f153c"></a>`unit`: `export`

### Declared structure

- <a id="s-69ecfa6993"></a>`kind`: `"constant"`
- <a id="s-592beaacde"></a>`value`: `100`

## Governing policies

- <a id="pa-d38dd4326d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CATALOG_SYNC_PAGE_SIZE_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56a63795e601d8432b510b689f923595f0e0228cc44895e5221b9bb372620617 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 100
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CATALOG_SYNC_PAGE_SIZE_MAX",
  "unit": "export"
}
```

</details>
