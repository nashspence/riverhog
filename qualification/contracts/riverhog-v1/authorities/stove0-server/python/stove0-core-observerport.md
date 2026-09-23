# stove0_core.ObserverPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-observerport:80546729f5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e6b3f344d6"></a>
- <a id="s-a507e005c7"></a>`distribution`: `stove0-server`
- <a id="s-c9be698de7"></a>`module`: `stove0_core`
- <a id="s-08e6b73137"></a>`name`: `ObserverPort`
- <a id="s-0bf7ba28b0"></a>`unit`: `export`

### Declared structure

- <a id="s-7ef1b73a1b"></a>`kind`: `"class"`
- <a id="s-3fa644038c"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [descriptor](stove0-core-observerport-descriptor.md)
- [observe](stove0-core-observerport-observe.md)

## Governing policies

- <a id="pa-2e818d3e3f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.ObserverPort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3b96fdf105972915ebe0636135483104b6005e1279e5b6f9935bc4e64aed6428 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ObserverPort",
  "unit": "export"
}
```

</details>
