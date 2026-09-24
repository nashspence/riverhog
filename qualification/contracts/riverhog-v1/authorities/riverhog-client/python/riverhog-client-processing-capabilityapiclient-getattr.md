# riverhog_client.processing.CapabilityApiClient.__getattr__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-capabilityapic-0faf5e9515:0a21928814 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2c66601c1c"></a>
- <a id="s-587781fb3b"></a>`distribution`: `riverhog-client`
- <a id="s-e655947168"></a>`module`: `riverhog_client.processing`
- <a id="s-8297c4b2e1"></a>`name`: `__getattr__`
- <a id="s-3ac5083f9f"></a>`owner`: `riverhog_client.processing.CapabilityApiClient`
- <a id="s-934c15ef33"></a>`unit`: `member`

### Declared structure

- <a id="s-344df5f624"></a>`kind`: `"method"`
- <a id="s-5cf8bc64db"></a>`signature`: `"\"(self, name: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [CapabilityApiClient](riverhog-client-processing-capabilityapiclient.md)

## Governing policies

- <a id="pa-9cbab014f8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CapabilityApiClient.__getattr__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1fcbfd91973c23f87e00fc7a8719de7762b6d74453f5a912993529a5c45f064f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, name: 'str') -> 'Any'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "__getattr__",
  "owner": "riverhog_client.processing.CapabilityApiClient",
  "unit": "member"
}
```

</details>
