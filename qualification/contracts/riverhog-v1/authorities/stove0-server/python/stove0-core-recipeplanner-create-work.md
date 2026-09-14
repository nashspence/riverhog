# stove0_core.RecipePlanner.create_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipeplanner-create-work:eb49a0e231 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-d4e50622c7"></a>`signature`: `"\"(self, recipe_id: 'str', roots: 'Sequence[CollectionRootRef]', *, revision: 'int \| None' = None, effective_intent: 'Mapping[str, JsonValue] \| None' = None) -> 'WorkIdentity'\""`

## Maintained corroboration

### Related interface records

- [RecipePlanner](stove0-core-recipeplanner.md)

## Governing policies

- <a id="pa-89333e0e0c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipePlanner.create_work`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc80422355214517934d7ccf09c40f01f30c19959b1c24e940b04113e407540d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, recipe_id: 'str', roots: 'Sequence[CollectionRootRef]', *, revision: 'int | None' = None, effective_intent: 'Mapping[str, JsonValue] | None' = None) -> 'WorkIdentity'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create_work",
  "owner": "stove0_core.RecipePlanner",
  "unit": "member"
}
```
