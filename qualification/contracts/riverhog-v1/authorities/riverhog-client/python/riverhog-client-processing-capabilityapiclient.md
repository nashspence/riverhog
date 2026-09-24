# riverhog_client.processing.CapabilityApiClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-capabilityapiclient:ca4f0d9ecb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-140a7b348e"></a>
- <a id="s-da24bccfe0"></a>`distribution`: `riverhog-client`
- <a id="s-85a6b02152"></a>`module`: `riverhog_client.processing`
- <a id="s-370182cd06"></a>`name`: `CapabilityApiClient`
- <a id="s-4b17ec61ba"></a>`unit`: `export`

### Declared structure

- <a id="s-6d9a1c43c7"></a>`kind`: `"class"`
- <a id="s-c1445e78b0"></a>`signature`: `"\"(client: 'Any', *, owns_client: 'bool' = False, _state: '_CapabilityClientState \| None' = None, _root: 'bool' = True) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [__getattr__](riverhog-client-processing-capabilityapiclient-getattr.md)
- [current](riverhog-client-processing-capabilityapiclient-current.md)
- [replace](riverhog-client-processing-capabilityapiclient-replace.md)
- [close](riverhog-client-processing-capabilityapiclient-close.md)
- [__enter__](riverhog-client-processing-capabilityapiclient-enter.md)
- [__exit__](riverhog-client-processing-capabilityapiclient-exit.md)
- [spawn](riverhog-client-processing-capabilityapiclient-spawn.md)

## Governing policies

- <a id="pa-745315ca09"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CapabilityApiClient`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e44853ba22126c16f73d27dacd14f2c5fa9c389c01791d36f9c0089bb815c2a1 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(client: 'Any', *, owns_client: 'bool' = False, _state: '_CapabilityClientState | None' = None, _root: 'bool' = True) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "CapabilityApiClient",
  "unit": "export"
}
```

</details>
