# riverhog_client.processing.ClaimedArtifact.key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedartifact-key:b447559dd3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e37dc057d5"></a>
- <a id="s-e0be5b3865"></a>`distribution`: `riverhog-client`
- <a id="s-3865ba2644"></a>`module`: `riverhog_client.processing`
- <a id="s-07c80b227d"></a>`name`: `key`
- <a id="s-c4e4fb0422"></a>`owner`: `riverhog_client.processing.ClaimedArtifact`
- <a id="s-6f6a6fa052"></a>`unit`: `member`

### Declared structure

- <a id="s-4cdf3120c5"></a>`kind`: `"property"`
- <a id="s-f8544dd5ff"></a>`signature`: `"\"(self) -> 'tuple[int, str]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedArtifact](riverhog-client-processing-claimedartifact.md)

## Governing policies

- <a id="pa-7f04be21e7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedArtifact.key`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 85d3ff97fe584d1d3bd2f8e99cb1cfe748236866190321d148c49b884f52e659 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'tuple[int, str]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "key",
  "owner": "riverhog_client.processing.ClaimedArtifact",
  "unit": "member"
}
```

</details>
