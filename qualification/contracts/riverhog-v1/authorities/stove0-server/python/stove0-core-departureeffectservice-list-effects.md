# stove0_core.DepartureEffectService.list_effects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-departureeffectservice-list-effects:33b88b7f55 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-38527a0e21"></a>
- <a id="s-b0716dc41c"></a>`distribution`: `stove0-server`
- <a id="s-992898a5f0"></a>`module`: `stove0_core`
- <a id="s-86aaf153f6"></a>`name`: `list_effects`
- <a id="s-534e3d2734"></a>`owner`: `stove0_core.DepartureEffectService`
- <a id="s-fe6aac98e4"></a>`unit`: `member`

### Declared structure

- <a id="s-4f17b6f272"></a>`kind`: `"method"`
- <a id="s-d2f78df535"></a>`signature`: `"\"(self, *, page_size: 'int' = 100, after_id: 'str \| None' = None) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectService](stove0-core-departureeffectservice.md)

## Governing policies

- <a id="pa-1bbe88fe1d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.DepartureEffectService.list_effects`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 632d40789695be2f5336e16f7a135a2327f848b3cd4fc809f293a5af890d4947 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 100, after_id: 'str | None' = None) -> 'dict[str, object]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "list_effects",
  "owner": "stove0_core.DepartureEffectService",
  "unit": "member"
}
```

</details>
