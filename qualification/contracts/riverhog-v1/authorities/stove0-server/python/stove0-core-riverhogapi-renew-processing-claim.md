# stove0_core.RiverhogApi.renew_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-renew-processing-claim:f132eeac41 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eb33bb93c6"></a>
- <a id="s-ff559b05cc"></a>`distribution`: `stove0-server`
- <a id="s-979cdea7c2"></a>`module`: `stove0_core`
- <a id="s-4320ca7fd6"></a>`name`: `renew_processing_claim`
- <a id="s-0624f3f4e3"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-5376519488"></a>`unit`: `member`

### Declared structure

- <a id="s-b6b6deb133"></a>`kind`: `"method"`
- <a id="s-fc0d2e2ea5"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int', lease_seconds: 'int' = 1800) -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-cd73cdfb16"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.renew_processing_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40fc649e7056c5f6e0a4abfe42d340446440255155d4175800fa6ebd76d779d3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int', lease_seconds: 'int' = 1800) -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "renew_processing_claim",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
