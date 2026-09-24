# riverhog_client.processing.CapabilityApiClient.__enter__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-capabilityapiclient-enter:dd91cf98c0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b7b7ac233f"></a>
- <a id="s-ab21a5048d"></a>`distribution`: `riverhog-client`
- <a id="s-89adbf3b03"></a>`module`: `riverhog_client.processing`
- <a id="s-ecee4f07c8"></a>`name`: `__enter__`
- <a id="s-64f6b3d90d"></a>`owner`: `riverhog_client.processing.CapabilityApiClient`
- <a id="s-70b3d43bee"></a>`unit`: `member`

### Declared structure

- <a id="s-b3ff93d631"></a>`kind`: `"method"`
- <a id="s-20d7310070"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [CapabilityApiClient](riverhog-client-processing-capabilityapiclient.md)

## Governing policies

- <a id="pa-97a1b56f10"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CapabilityApiClient.__enter__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 38d09f633b9a1afe8a57a3a61cda6186a55e580767f80d6ea06afc3caf433065 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "__enter__",
  "owner": "riverhog_client.processing.CapabilityApiClient",
  "unit": "member"
}
```

</details>
