# stove0_core.TargetCallbackAuthority.declare_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetcallbackauthority-declare-output:9793fc04ef -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8f10e713d6"></a>
- <a id="s-6c07f06030"></a>`distribution`: `stove0-server`
- <a id="s-c8ce30ef5e"></a>`module`: `stove0_core`
- <a id="s-67b470c22e"></a>`name`: `declare_output`
- <a id="s-8fc8ec1a1e"></a>`owner`: `stove0_core.TargetCallbackAuthority`
- <a id="s-5013b5e21c"></a>`unit`: `member`

### Declared structure

- <a id="s-2860819728"></a>`kind`: `"method"`
- <a id="s-c296a5bfd6"></a>`signature`: `"\"(self, token: 'str', *, job_id: 'str', output: 'OutputArtifact') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TargetCallbackAuthority](stove0-core-targetcallbackauthority.md)

## Governing policies

- <a id="pa-32a42e4d05"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TargetCallbackAuthority.declare_output`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f8d5c3c96f0fe04bc09a78f6f97b5c42a88ef6a13fd026864b61e66323d4a5f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, token: 'str', *, job_id: 'str', output: 'OutputArtifact') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "declare_output",
  "owner": "stove0_core.TargetCallbackAuthority",
  "unit": "member"
}
```

</details>
