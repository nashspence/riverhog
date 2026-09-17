# stove0_core.HttpObserverPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httpobserverport:71e6ea71e6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63cd70f1f9"></a>
- <a id="s-40c06f568f"></a>`distribution`: `stove0-server`
- <a id="s-f1a60cdb6b"></a>`module`: `stove0_core`
- <a id="s-c1604bd159"></a>`name`: `HttpObserverPort`
- <a id="s-f18f35f134"></a>`unit`: `export`

### Declared structure

- <a id="s-f9a61f303c"></a>`kind`: `"class"`
- <a id="s-e7f428eaa8"></a>`signature`: `"\"(registrations: 'dict[str, ContentObserverClient]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [descriptor](stove0-core-httpobserverport-descriptor.md)
- [observe](stove0-core-httpobserverport-observe.md)

## Governing policies

- <a id="pa-040925d210"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.HttpObserverPort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8fc664c3aa71e220e4602112ce77bc006676d715abe2b8908d4b30fb5f65f2e0 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(registrations: 'dict[str, ContentObserverClient]') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "HttpObserverPort",
  "unit": "export"
}
```

</details>
