# riverhog_client.processing.CapabilityApiClient.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-capabilityapiclient-close:08ea07f212 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b69774c96"></a>
- <a id="s-bd3540b50a"></a>`distribution`: `riverhog-client`
- <a id="s-d17548b2f0"></a>`module`: `riverhog_client.processing`
- <a id="s-bff07c8f2e"></a>`name`: `close`
- <a id="s-beaf74f370"></a>`owner`: `riverhog_client.processing.CapabilityApiClient`
- <a id="s-cecb027f08"></a>`unit`: `member`

### Declared structure

- <a id="s-cb065a438b"></a>`kind`: `"method"`
- <a id="s-4fb48a329c"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [CapabilityApiClient](riverhog-client-processing-capabilityapiclient.md)

## Governing policies

- <a id="pa-97211d8c73"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CapabilityApiClient.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b2532c72a1593c173a61f9dca02b301485f3839cfb9479c0446492faca8636e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "close",
  "owner": "riverhog_client.processing.CapabilityApiClient",
  "unit": "member"
}
```

</details>
