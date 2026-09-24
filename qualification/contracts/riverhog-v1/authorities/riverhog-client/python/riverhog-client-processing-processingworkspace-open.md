# riverhog_client.processing.ProcessingWorkspace.open

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-processingworkspace-open:e661e028df -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b7ce278f1f"></a>
- <a id="s-b822828e5e"></a>`distribution`: `riverhog-client`
- <a id="s-2e1a29e529"></a>`module`: `riverhog_client.processing`
- <a id="s-d6f6662112"></a>`name`: `open`
- <a id="s-a89ffbbc0c"></a>`owner`: `riverhog_client.processing.ProcessingWorkspace`
- <a id="s-10212628b8"></a>`unit`: `member`

### Declared structure

- <a id="s-bcf1aaee12"></a>`kind`: `"classmethod"`
- <a id="s-24ef2596cb"></a>`signature`: `"\"(cls, root: 'Path', *, execution_id: 'str', declared_protection: 'DeclaredWorkspaceProtection') -> 'ProcessingWorkspace'\""`

## Maintained corroboration

### Related interface records

- [ProcessingWorkspace](riverhog-client-processing-processingworkspace.md)

## Governing policies

- <a id="pa-7c5bbd498c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ProcessingWorkspace.open`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae3a66a1bff1c53045345b94514f640eed1663a679c96ef41558f271921649dd -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, root: 'Path', *, execution_id: 'str', declared_protection: 'DeclaredWorkspaceProtection') -> 'ProcessingWorkspace'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "open",
  "owner": "riverhog_client.processing.ProcessingWorkspace",
  "unit": "member"
}
```

</details>
