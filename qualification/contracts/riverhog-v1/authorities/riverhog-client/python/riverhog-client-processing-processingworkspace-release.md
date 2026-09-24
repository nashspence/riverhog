# riverhog_client.processing.ProcessingWorkspace.release

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-processingwork-09b28649c6:77f5c323ee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fa88eaac7f"></a>
- <a id="s-7e328dce9a"></a>`distribution`: `riverhog-client`
- <a id="s-f5148afb26"></a>`module`: `riverhog_client.processing`
- <a id="s-46afa7de04"></a>`name`: `release`
- <a id="s-70a4ff35bf"></a>`owner`: `riverhog_client.processing.ProcessingWorkspace`
- <a id="s-f4a4563bcd"></a>`unit`: `member`

### Declared structure

- <a id="s-235ba4ecc7"></a>`kind`: `"method"`
- <a id="s-7a13e5b17f"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ProcessingWorkspace](riverhog-client-processing-processingworkspace.md)

## Governing policies

- <a id="pa-57b8188867"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ProcessingWorkspace.release`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd296578a75371fcbb60fd0e6c75db5e1e3d853cc8ff676bf5fc1c39299455f0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "release",
  "owner": "riverhog_client.processing.ProcessingWorkspace",
  "unit": "member"
}
```

</details>
