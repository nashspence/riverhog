# stove0_observer_support.ContentObservationRuntime.read_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-b47642a7ad:4ceeb3bacb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2423d65670"></a>
- <a id="s-c06191f4b4"></a>`distribution`: `stove0-observer-support`
- <a id="s-3bca069803"></a>`module`: `stove0_observer_support`
- <a id="s-f4023c70d4"></a>`name`: `read_bytes`
- <a id="s-3b737d2fb4"></a>`owner`: `stove0_observer_support.ContentObservationRuntime`
- <a id="s-7f52ff47b8"></a>`unit`: `member`

### Declared structure

- <a id="s-2b400baed8"></a>`kind`: `"method"`
- <a id="s-80859eb6f0"></a>`signature`: `"\"(self, subject: 'WorkArtifactSubject', *, maximum_bytes: 'int', **prepare_kwargs: 'Any') -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRuntime](stove0-observer-support-contentobservationruntime.md)

## Governing policies

- <a id="pa-9f04728726"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationRuntime.read_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68e3ee4e73fb42cf533439992d9c0aed6c7dd38ca7d8bc30a66b0a0cb4ba2e5d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, subject: 'WorkArtifactSubject', *, maximum_bytes: 'int', **prepare_kwargs: 'Any') -> 'bytes'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "read_bytes",
  "owner": "stove0_observer_support.ContentObservationRuntime",
  "unit": "member"
}
```

</details>
