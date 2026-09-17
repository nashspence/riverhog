# riverhog_archive_contracts.SegmentArchiveVolume.source_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-segmentarchive-b015f923b2:74a8f93acf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ad52efab20"></a>
- <a id="s-5bc014c709"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-a743452bfb"></a>`module`: `riverhog_archive_contracts`
- <a id="s-664ba17d66"></a>`name`: `source_file`
- <a id="s-2cd1d42e56"></a>`owner`: `riverhog_archive_contracts.SegmentArchiveVolume`
- <a id="s-47954e2498"></a>`unit`: `member`

### Declared structure

- <a id="s-4c80d7e94a"></a>`kind`: `"property"`
- <a id="s-e30222b142"></a>`signature`: `"\"(self) -> 'ArchiveFileIdentity'\""`

## Maintained corroboration

### Related interface records

- [SegmentArchiveVolume](riverhog-archive-contracts-segmentarchivevolume.md)

## Governing policies

- <a id="pa-ff95c9d0f9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.SegmentArchiveVolume.source_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09c97a8ab90bdbab99a8bd85684be127d4de147c068932919c5cc1646394ad5f -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'ArchiveFileIdentity'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "source_file",
  "owner": "riverhog_archive_contracts.SegmentArchiveVolume",
  "unit": "member"
}
```

</details>
