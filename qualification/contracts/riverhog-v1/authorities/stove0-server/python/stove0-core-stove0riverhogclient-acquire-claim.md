# stove0_core.Stove0RiverhogClient.acquire_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-acquire-claim:7a62257aeb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bfc5463c2e"></a>
- <a id="s-7093cc0707"></a>`distribution`: `stove0-server`
- <a id="s-890b2fd905"></a>`module`: `stove0_core`
- <a id="s-31e48d7c8b"></a>`name`: `acquire_claim`
- <a id="s-5f02f7137c"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-47a1b95865"></a>`unit`: `member`

### Declared structure

- <a id="s-16643f61af"></a>`kind`: `"method"`
- <a id="s-01e90b3d31"></a>`signature`: `"\"(self, work: 'WorkIdentity') -> 'ClaimBinding'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-2080b939c2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.acquire_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2ed87997b1f011f2c07bb784291e09c44f4c5c7e682f18dd3f3e53cd89d9de1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity') -> 'ClaimBinding'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "acquire_claim",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```

</details>
