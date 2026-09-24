# stove0_core.RecipePlanner.create_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipeplanner-create-work:eb49a0e231 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-172e3ffe0e"></a>
- <a id="s-f5dfb070cd"></a>`distribution`: `stove0-server`
- <a id="s-44fa50fb3d"></a>`module`: `stove0_core`
- <a id="s-16b6be9563"></a>`name`: `create_work`
- <a id="s-801b611672"></a>`owner`: `stove0_core.RecipePlanner`
- <a id="s-f4784696b3"></a>`unit`: `member`

### Declared structure

- <a id="s-a877b11054"></a>`kind`: `"method"`
- <a id="s-d4e50622c7"></a>`signature`: `"\"(self, recipe_id: 'str', roots: 'Sequence[CollectionRootIdentityRef]', *, revision: 'int \| None' = None, effective_intent: 'Mapping[str, JsonValue] \| None' = None) -> 'WorkIdentity'\""`

## Maintained corroboration

### Related interface records

- [RecipePlanner](stove0-core-recipeplanner.md)

## Governing policies

- <a id="pa-89333e0e0c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipePlanner.create_work`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c66104ddc5807b5e389d9a02f9b6a8578b8fa820903e70fa1bef080a865d3ee7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, recipe_id: 'str', roots: 'Sequence[CollectionRootIdentityRef]', *, revision: 'int | None' = None, effective_intent: 'Mapping[str, JsonValue] | None' = None) -> 'WorkIdentity'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create_work",
  "owner": "stove0_core.RecipePlanner",
  "unit": "member"
}
```

</details>
