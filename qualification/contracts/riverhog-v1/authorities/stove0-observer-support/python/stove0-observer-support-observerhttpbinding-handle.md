# stove0_observer_support.ObserverHttpBinding.handle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observerhttpbinding-handle:38a3a9deec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-487f43e605"></a>
- <a id="s-32cf6f0c6d"></a>`distribution`: `stove0-observer-support`
- <a id="s-c8f1b557d8"></a>`module`: `stove0_observer_support`
- <a id="s-7bf5edfc05"></a>`name`: `handle`
- <a id="s-ebbf4ba334"></a>`owner`: `stove0_observer_support.ObserverHttpBinding`
- <a id="s-0c481c5124"></a>`unit`: `member`

### Declared structure

- <a id="s-83fe3e9e92"></a>`kind`: `"method"`
- <a id="s-18959979df"></a>`signature`: `"\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'ObserverHttpResponse'\""`

## Maintained corroboration

### Related interface records

- [ObserverHttpBinding](stove0-observer-support-observerhttpbinding.md)

## Governing policies

- <a id="pa-5d00f0601c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObserverHttpBinding.handle`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c57290ce3c94c6ad74ed4caf826b1fa5f5ee77eb169e6250107fd778452dea64 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'ObserverHttpResponse'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "handle",
  "owner": "stove0_observer_support.ObserverHttpBinding",
  "unit": "member"
}
```

</details>
