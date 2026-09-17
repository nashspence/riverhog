# stove0_observer_support.CancellationCheck

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-cancellationcheck:33c49f278c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-277bf3ff06"></a>
- <a id="s-4db7d9d1a6"></a>`distribution`: `stove0-observer-support`
- <a id="s-ee5fdd30b0"></a>`module`: `stove0_observer_support`
- <a id="s-cf172b8bfa"></a>`name`: `CancellationCheck`
- <a id="s-483494b6dd"></a>`unit`: `export`

### Declared structure

- <a id="s-6581c130b8"></a>`kind`: `"object"`
- <a id="s-fef6e867d0"></a>`type`: `"collections.abc._CallableGenericAlias"`

## Governing policies

- <a id="pa-d76eab7086"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.CancellationCheck`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08db1d359e814ee8d1882cd3ae99b3500a63d1bdcf6475c1aad59a1d016744d5 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "collections.abc._CallableGenericAlias"
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "CancellationCheck",
  "unit": "export"
}
```

</details>
