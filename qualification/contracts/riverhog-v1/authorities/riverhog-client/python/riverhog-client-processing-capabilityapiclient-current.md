# riverhog_client.processing.CapabilityApiClient.current

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-capabilityapic-92ca564166:238532c1e6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-70ba0f928d"></a>
- <a id="s-4418faacff"></a>`distribution`: `riverhog-client`
- <a id="s-3daa95d615"></a>`module`: `riverhog_client.processing`
- <a id="s-f2db16a3df"></a>`name`: `current`
- <a id="s-0e1fba1c08"></a>`owner`: `riverhog_client.processing.CapabilityApiClient`
- <a id="s-a10c83ca34"></a>`unit`: `member`

### Declared structure

- <a id="s-3e31f03877"></a>`kind`: `"property"`
- <a id="s-e3c468909f"></a>`signature`: `"\"(self) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [CapabilityApiClient](riverhog-client-processing-capabilityapiclient.md)

## Governing policies

- <a id="pa-1372da490d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CapabilityApiClient.current`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 805de1427390c8eae9468188e8c949ac9baeac093369ce5093fbdd5c7d67a8ab -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'Any'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "current",
  "owner": "riverhog_client.processing.CapabilityApiClient",
  "unit": "member"
}
```

</details>
