# stove0_core.RiverhogControlPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport:93bd3c0f7f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-310eb56c1e"></a>
| Field | Shape |
|---|---|
| <a id="s-0a5fc66a39"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-2cb16a9468"></a>`distribution` | "stove0-server" |
| <a id="s-e1eb4a1a3d"></a>`module` | "stove0_core" |
| <a id="s-d519e665e5"></a>`name` | "RiverhogControlPort" |
| <a id="s-375a54df25"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogControlPort.abandon_claim](stove0-core-riverhogcontrolport-abandon-claim.md)
- [stove0_core.RiverhogControlPort.acquire_claim](stove0-core-riverhogcontrolport-acquire-claim.md)
- [stove0_core.RiverhogControlPort.begin_retirement](stove0-core-riverhogcontrolport-begin-retirement.md)
- [stove0_core.RiverhogControlPort.observation_authority](stove0-core-riverhogcontrolport-observation-authority.md)
- [stove0_core.RiverhogControlPort.release_claim](stove0-core-riverhogcontrolport-release-claim.md)
- [stove0_core.RiverhogControlPort.renew_claim](stove0-core-riverhogcontrolport-renew-claim.md)
- [stove0_core.RiverhogControlPort.restart_claim](stove0-core-riverhogcontrolport-restart-claim.md)
- [stove0_core.RiverhogControlPort.retire_input](stove0-core-riverhogcontrolport-retire-input.md)
- [stove0_core.RiverhogControlPort.seal_execution](stove0-core-riverhogcontrolport-seal-execution.md)
- [stove0_core.RiverhogControlPort.settle_outcomes](stove0-core-riverhogcontrolport-settle-outcomes.md)
- [stove0_core.RiverhogControlPort.target_authority](stove0-core-riverhogcontrolport-target-authority.md)
- [stove0_core.RiverhogControlPort.verify_and_settle](stove0-core-riverhogcontrolport-verify-and-settle.md)

## Governing policies

- <a id="pa-bc44f72666"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort`

### Exact owned JSON

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
