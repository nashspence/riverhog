# riverhog_recover.RecoveryError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-recover:riverhog-recover-recoveryerror:0551187dc5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9eeb66482e"></a>
- <a id="s-65f78ac3b9"></a>`distribution`: `riverhog-recover`
- <a id="s-0136d233f2"></a>`module`: `riverhog_recover`
- <a id="s-544505e2e2"></a>`name`: `RecoveryError`
- <a id="s-e106abd745"></a>`unit`: `export`

### Declared structure

- <a id="s-e78573c006"></a>`kind`: `"class"`
- <a id="s-e990fc7308"></a>`signature`: `"unavailable"`

## Governing policies

- <a id="pa-af0a945bf2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-recover:riverhog_recover](../../../evidence/sources/authorities.md#src-dbfe6c5e2e) — [reference/riverhog/recovery/src/riverhog\_recover/\_\_init\_\_.py](../../../../../../reference/riverhog/recovery/src/riverhog_recover/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_recover.RecoveryError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 895294b88259a6700fdf509cdd1edbc70a6ecff4f979e0833f8021b5639aac39 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "unavailable"
  },
  "distribution": "riverhog-recover",
  "module": "riverhog_recover",
  "name": "RecoveryError",
  "unit": "export"
}
```

</details>
