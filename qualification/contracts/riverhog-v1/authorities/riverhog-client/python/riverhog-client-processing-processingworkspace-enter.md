# riverhog_client.processing.ProcessingWorkspace.__enter__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-processingworkspace-enter:faad2c9abb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bb48f2097d"></a>
- <a id="s-972962f9f4"></a>`distribution`: `riverhog-client`
- <a id="s-931f20f508"></a>`module`: `riverhog_client.processing`
- <a id="s-2063cf4be1"></a>`name`: `__enter__`
- <a id="s-a0305f7e02"></a>`owner`: `riverhog_client.processing.ProcessingWorkspace`
- <a id="s-d96e9a3e5e"></a>`unit`: `member`

### Declared structure

- <a id="s-e2a8551f68"></a>`kind`: `"method"`
- <a id="s-82ff9f6caa"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ProcessingWorkspace](riverhog-client-processing-processingworkspace.md)

## Governing policies

- <a id="pa-e431430198"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ProcessingWorkspace.__enter__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a92cdcbca674f26935cd2ca46b020abe61f92fadef9311a56d2f7403aabdc2e7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "__enter__",
  "owner": "riverhog_client.processing.ProcessingWorkspace",
  "unit": "member"
}
```

</details>
