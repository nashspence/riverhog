# stove0_observer_support.ObserverClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observerclient:9cf305a6af -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0b83b1421c"></a>
- <a id="s-f9aca99dbd"></a>`distribution`: `stove0-observer-support`
- <a id="s-7e2d64fb7f"></a>`module`: `stove0_observer_support`
- <a id="s-72cdaa1b9c"></a>`name`: `ObserverClient`
- <a id="s-7bc0aaf811"></a>`unit`: `export`

### Declared structure

- <a id="s-ae6dfb7c0e"></a>`kind`: `"class"`
- <a id="s-8fa99b8261"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [descriptor](stove0-observer-support-observerclient-descriptor.md)
- [observe](stove0-observer-support-observerclient-observe.md)

## Governing policies

- <a id="pa-8eba18a16e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObserverClient`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4a606cb46266b84050846633555ad0a48cd1141204514bdc2d3ed83a198c4939 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "ObserverClient",
  "unit": "export"
}
```

</details>
