# stove0_core.RiverhogControlPort.acquire_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-acquire-claim:087573aa5f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ab73ecedd9"></a>
- <a id="s-840c591ba3"></a>`distribution`: `stove0-server`
- <a id="s-2f0e9fdeea"></a>`module`: `stove0_core`
- <a id="s-bd1f4e0244"></a>`name`: `acquire_claim`
- <a id="s-72c7fd472b"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-f7ba701167"></a>`unit`: `member`

### Declared structure

- <a id="s-55320c0d00"></a>`kind`: `"method"`
- <a id="s-8a4a28f14b"></a>`signature`: `"\"(self, work: 'WorkIdentity') -> 'ClaimBinding'\""`

## Maintained corroboration

### Related interface records

- [RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-59b031dff5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.acquire_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b36aa83a2722459a115fce999dd24a6c154fd4c641c069ff96239179be03ae38 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity') -> 'ClaimBinding'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "acquire_claim",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```

</details>
