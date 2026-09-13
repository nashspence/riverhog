# stove0_core.RiverhogControlPort.renew_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-renew-claim:688ea0358b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ac266755fb"></a>
| Field | Shape |
|---|---|
| <a id="s-43ba54446a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-0626e4a792"></a>`distribution` | "stove0-server" |
| <a id="s-5635f6a853"></a>`module` | "stove0_core" |
| <a id="s-f773adf499"></a>`name` | "renew_claim" |
| <a id="s-3abc498bee"></a>`owner` | "stove0_core.RiverhogControlPort" |
| <a id="s-2bcea6c4b5"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-4f704bf406"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.renew_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 829f5fa777a246b95f35b764fd0ac4661b3e4fa90326500739f1481d4130b517 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity', claim: 'ClaimBinding') -> 'ClaimBinding'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "renew_claim",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```
