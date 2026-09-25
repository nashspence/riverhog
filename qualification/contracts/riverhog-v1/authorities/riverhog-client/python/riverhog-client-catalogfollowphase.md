# riverhog_client.CatalogFollowPhase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogfollowphase:ec14db07a8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4cfcd74ef"></a>
- <a id="s-e236d4dcb4"></a>`distribution`: `riverhog-client`
- <a id="s-1bb749f4c6"></a>`module`: `riverhog_client`
- <a id="s-c83250ae1e"></a>`name`: `CatalogFollowPhase`
- <a id="s-cdf3b55f3b"></a>`unit`: `export`

### Declared structure

- <a id="s-305ae65e61"></a>`kind`: `"type-alias"`
- <a id="s-4843c9b2d9"></a>`value`: `"typing.Literal['new', 'catalog', 'catchup', 'following', 'reset_required']"`

## Governing policies

- <a id="pa-542232c383"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogFollowPhase`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 374426fa4ae5cdd932badd423f127f4986fd8e159a1597d01a035f4e905e3e3e -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['new', 'catalog', 'catchup', 'following', 'reset_required']"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CatalogFollowPhase",
  "unit": "export"
}
```

</details>
