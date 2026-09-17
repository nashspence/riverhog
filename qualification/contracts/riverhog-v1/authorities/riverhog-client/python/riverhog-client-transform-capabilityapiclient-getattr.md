# riverhog_client.transform.CapabilityApiClient.__getattr__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-capabilityapicl-1565f3858a:2b603f9baf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7326daebb9"></a>
- <a id="s-810793700b"></a>`distribution`: `riverhog-client`
- <a id="s-37dbb4095e"></a>`module`: `riverhog_client.transform`
- <a id="s-d7efc78c75"></a>`name`: `__getattr__`
- <a id="s-8b6ea42daf"></a>`owner`: `riverhog_client.transform.CapabilityApiClient`
- <a id="s-72503ec708"></a>`unit`: `member`

### Declared structure

- <a id="s-85202fee66"></a>`kind`: `"method"`
- <a id="s-7280f331ed"></a>`signature`: `"\"(self, name: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [CapabilityApiClient](riverhog-client-transform-capabilityapiclient.md)

## Governing policies

- <a id="pa-afb924b408"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.CapabilityApiClient.__getattr__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 20eb315951bd15d271d9762f8e9fe1d6eed2b514fc7371677365f1c001d80030 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, name: 'str') -> 'Any'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "__getattr__",
  "owner": "riverhog_client.transform.CapabilityApiClient",
  "unit": "member"
}
```

</details>
