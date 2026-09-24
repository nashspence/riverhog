# riverhog_client.processing.CapabilityApiClient.spawn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-capabilityapiclient-spawn:e8d2b951f4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1bc6906e9f"></a>
- <a id="s-919c65d03b"></a>`distribution`: `riverhog-client`
- <a id="s-883cf1c566"></a>`module`: `riverhog_client.processing`
- <a id="s-44d19a7570"></a>`name`: `spawn`
- <a id="s-bdd57b124a"></a>`owner`: `riverhog_client.processing.CapabilityApiClient`
- <a id="s-dc49c2e08f"></a>`unit`: `member`

### Declared structure

- <a id="s-d7e87f54c4"></a>`kind`: `"method"`
- <a id="s-4ffc61cc64"></a>`signature`: `"\"(self) -> 'CapabilityApiClient'\""`

## Maintained corroboration

### Related interface records

- [CapabilityApiClient](riverhog-client-processing-capabilityapiclient.md)

## Governing policies

- <a id="pa-7264ebb572"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CapabilityApiClient.spawn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffae1710a5737e22edad503a333762b48ff33b1a92766b4515591678d588f8a8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'CapabilityApiClient'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "spawn",
  "owner": "riverhog_client.processing.CapabilityApiClient",
  "unit": "member"
}
```

</details>
