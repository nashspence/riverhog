# riverhog_client.processing.ProcessingWorkspace.resolve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-processingwork-39ac66932b:559ae4b7a1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ba7ad0b445"></a>
- <a id="s-b51a703e2e"></a>`distribution`: `riverhog-client`
- <a id="s-a42fb6d6fd"></a>`module`: `riverhog_client.processing`
- <a id="s-8125b66088"></a>`name`: `resolve`
- <a id="s-555554cd13"></a>`owner`: `riverhog_client.processing.ProcessingWorkspace`
- <a id="s-fe3ed48222"></a>`unit`: `member`

### Declared structure

- <a id="s-80fc25d2e2"></a>`kind`: `"method"`
- <a id="s-aa9fa1e305"></a>`signature`: `"\"(self, relative_path: 'str') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [ProcessingWorkspace](riverhog-client-processing-processingworkspace.md)

## Governing policies

- <a id="pa-287d3a501d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ProcessingWorkspace.resolve`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 256ac462cbbb28a4e1a62a45cbbe8c94c1aa332536c78d9ead2ce6ebb91459d1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, relative_path: 'str') -> 'Path'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "resolve",
  "owner": "riverhog_client.processing.ProcessingWorkspace",
  "unit": "member"
}
```

</details>
