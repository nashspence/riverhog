# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `b54dad8f2f96c081905194d9ba3ceb40c1a4ea7af6a14ce4bfc954da2b979a0e` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `6517fb54c549030854e2ae133f25e46a924b442e5a02375d806a7349bac07baa` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `567c3f48d8427332e628d1e05101b7039223d4c7c612b2a5ae0e887fd424da95` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `a6876686fc56197b1ada7df3142e3337c7cf98b7e9d9e40751a9fa6d55ba7737` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `f4605c5ba7a80fceacf3b1ddf3a0df0abc5d66455beff58ec8fa43676fc254c7` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
