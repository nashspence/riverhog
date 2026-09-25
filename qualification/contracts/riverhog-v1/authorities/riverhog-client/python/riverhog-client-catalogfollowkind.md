# riverhog_client.CatalogFollowKind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogfollowkind:24220bd6d6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ad1ca082e4"></a>
- <a id="s-ccda66707c"></a>`distribution`: `riverhog-client`
- <a id="s-02ff5cac9d"></a>`module`: `riverhog_client`
- <a id="s-4edc4c01d7"></a>`name`: `CatalogFollowKind`
- <a id="s-cf71360404"></a>`unit`: `export`

### Declared structure

- <a id="s-2c3be4180a"></a>`kind`: `"type-alias"`
- <a id="s-cf2e09e7e5"></a>`value`: `"typing.Literal['checkpoint', 'catalog', 'changes', 'reset']"`

## Governing policies

- <a id="pa-f8696d838b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogFollowKind`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82218915b16e40ed7e7c1cda7278b1b6f9078a3f239e7af4257b05ce1880121f -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['checkpoint', 'catalog', 'changes', 'reset']"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CatalogFollowKind",
  "unit": "export"
}
```

</details>
