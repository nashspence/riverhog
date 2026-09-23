# stove0_core.RiverhogControlPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport:93bd3c0f7f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-310eb56c1e"></a>
- <a id="s-2cb16a9468"></a>`distribution`: `stove0-server`
- <a id="s-e1eb4a1a3d"></a>`module`: `stove0_core`
- <a id="s-d519e665e5"></a>`name`: `RiverhogControlPort`
- <a id="s-375a54df25"></a>`unit`: `export`

### Declared structure

- <a id="s-e6bfe8c215"></a>`kind`: `"class"`
- <a id="s-2498de00c5"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [abandon_claim](stove0-core-riverhogcontrolport-abandon-claim.md)
- [acquire_claim](stove0-core-riverhogcontrolport-acquire-claim.md)
- [begin_retirement](stove0-core-riverhogcontrolport-begin-retirement.md)
- [observation_authority](stove0-core-riverhogcontrolport-observation-authority.md)
- [release_claim](stove0-core-riverhogcontrolport-release-claim.md)
- [renew_claim](stove0-core-riverhogcontrolport-renew-claim.md)
- [restart_claim](stove0-core-riverhogcontrolport-restart-claim.md)
- [retire_input](stove0-core-riverhogcontrolport-retire-input.md)
- [seal_execution](stove0-core-riverhogcontrolport-seal-execution.md)
- [settle_outcomes](stove0-core-riverhogcontrolport-settle-outcomes.md)
- [target_authority](stove0-core-riverhogcontrolport-target-authority.md)
- [verify_and_settle](stove0-core-riverhogcontrolport-verify-and-settle.md)

## Governing policies

- <a id="pa-bc44f72666"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89f14ae96452c8563e7323908959f7917b64f626d8e4f3daf53c77503bcb8fca -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "RiverhogControlPort",
  "unit": "export"
}
```

</details>
