# riverhog_client.processing.CapabilityApiClient.replace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-capabilityapic-cd2248d3c1:aed99d9f93 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7bb111ce94"></a>
- <a id="s-df8dda7d02"></a>`distribution`: `riverhog-client`
- <a id="s-e2af2fbe3e"></a>`module`: `riverhog_client.processing`
- <a id="s-93a1cfaf64"></a>`name`: `replace`
- <a id="s-a3cf249198"></a>`owner`: `riverhog_client.processing.CapabilityApiClient`
- <a id="s-1a018130e0"></a>`unit`: `member`

### Declared structure

- <a id="s-ce7f3f029b"></a>`kind`: `"method"`
- <a id="s-2ad0c231ab"></a>`signature`: `"\"(self, client: 'Any', *, owns_client: 'bool' = True) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [CapabilityApiClient](riverhog-client-processing-capabilityapiclient.md)

## Governing policies

- <a id="pa-93c5ca6f77"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CapabilityApiClient.replace`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30f6e1aa2e21d91b9604e0e855b12439ef1971229b1d5d6ca0c0cf841044efb2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, client: 'Any', *, owns_client: 'bool' = True) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "replace",
  "owner": "riverhog_client.processing.CapabilityApiClient",
  "unit": "member"
}
```

</details>
