# riverhog_client.processing.ProcessingWorkspace.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-processingworkspace-exit:8849cfedaf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f9dcb352e3"></a>
- <a id="s-c59b492e4f"></a>`distribution`: `riverhog-client`
- <a id="s-86656da79a"></a>`module`: `riverhog_client.processing`
- <a id="s-ab62dac71f"></a>`name`: `__exit__`
- <a id="s-4429bc0618"></a>`owner`: `riverhog_client.processing.ProcessingWorkspace`
- <a id="s-cb503edbb8"></a>`unit`: `member`

### Declared structure

- <a id="s-a0d36a6c32"></a>`kind`: `"method"`
- <a id="s-fb814abe27"></a>`signature`: `"\"(self, *_args: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ProcessingWorkspace](riverhog-client-processing-processingworkspace.md)

## Governing policies

- <a id="pa-cb99dde635"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ProcessingWorkspace.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8a284deb40a190f9052980f595da64ca707cc23dd0a275e9322fde87b50e2fd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *_args: 'object') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "__exit__",
  "owner": "riverhog_client.processing.ProcessingWorkspace",
  "unit": "member"
}
```

</details>
