# riverhog_client.processing.CollectionTransformRuntime.refresh_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-collectiontran-aab79240ce:1849124dac -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-46901bf1ef"></a>
- <a id="s-df6d5e686f"></a>`distribution`: `riverhog-client`
- <a id="s-2ae90c2909"></a>`module`: `riverhog_client.processing`
- <a id="s-5c8ba3b1d2"></a>`name`: `refresh_capability`
- <a id="s-b9089873f9"></a>`owner`: `riverhog_client.processing.CollectionTransformRuntime`
- <a id="s-7db0c502f4"></a>`unit`: `member`

### Declared structure

- <a id="s-20b2b08ea0"></a>`kind`: `"method"`
- <a id="s-9cec79c713"></a>`signature`: `"\"(self, capability_token: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-processing-collectiontransformruntime.md)

## Governing policies

- <a id="pa-9288ed2b05"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CollectionTransformRuntime.refresh_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c77d57639ca15392e1f04f88f227e5b5180a632491b5c6557b28e7913eca106 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, capability_token: 'str') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "refresh_capability",
  "owner": "riverhog_client.processing.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
