# Riverhog repository v1 contract audit

> **Audit question:** Is this exactly the external contract the Riverhog repository should support for v1 — no more, no less?

**Audit path:** Scope → Semantics → Evidence

## Contract map

### Riverhog product

- [Riverhog service](surfaces/riverhog.md#riverhog-service) — 1 authority, 471 contract elements
  - [riverhog](authorities/riverhog/index.md) — 471 contract elements
- [Riverhog-owned contracts and libraries](surfaces/riverhog.md#riverhog-owned-contracts-and-libraries) — 15 authorities, 67 contract elements
  - [http-api-contracts](authorities/http-api-contracts/index.md) — 2 contract elements
  - [lifecycle-events](authorities/lifecycle-events/index.md) — 2 contract elements
  - [riverhog-age](authorities/riverhog-age/index.md) — 2 contract elements
  - [riverhog-application-access](authorities/riverhog-application-access/index.md) — 2 contract elements
  - [riverhog-archive-contracts](authorities/riverhog-archive-contracts/index.md) — 6 contract elements
  - [riverhog-client](authorities/riverhog-client/index.md) — 3 contract elements
  - [riverhog-protocol](authorities/riverhog-protocol/index.md) — 3 contract elements
  - [riverhog-provenance](authorities/riverhog-provenance/index.md) — 10 contract elements
  - [riverhog-provenance-contracts](authorities/riverhog-provenance-contracts/index.md) — 3 contract elements
  - [riverhog-provenance-installation](authorities/riverhog-provenance-installation/index.md) — 1 contract element
  - [riverhog-storage-adapter-asgi-support](authorities/riverhog-storage-adapter-asgi-support/index.md) — 2 contract elements
  - [riverhog-storage-adapter-conformance](authorities/riverhog-storage-adapter-conformance/index.md) — 1 contract element
  - [riverhog-storage-adapter-protocol](authorities/riverhog-storage-adapter-protocol/index.md) — 3 contract elements
  - [riverhog-storage-adapter-schemas](authorities/riverhog-storage-adapter-schemas/index.md) — 1 contract element
  - [riverhog-storage-adapter-support](authorities/riverhog-storage-adapter-support/index.md) — 26 contract elements
- [Riverhog extension boundaries](surfaces/riverhog.md#extension-boundaries) — 0 authorities, 0 contract elements
  - `riverhog-storage-adapter`
  - `riverhog.provenance-contracts`
  - `riverhog.provenance-observers`
- [Implementation and build](surfaces/riverhog.md#implementation-and-build) — 5 authorities, 5 contract elements
  - [config-validation](authorities/config-validation/index.md) — 1 contract element
  - [riverhog-catalog](authorities/riverhog-catalog/index.md) — 1 contract element
  - [riverhog-server](authorities/riverhog-server/index.md) — 1 contract element
  - [state-schema](authorities/state-schema/index.md) — 1 contract element
  - [time-formats](authorities/time-formats/index.md) — 1 contract element

### Maintainer-selected nonnormative references

- [Riverhog references](surfaces/references.md#riverhog-references) — 15 authorities, 60 contract elements
  - [riverhog-ftp-adapter](authorities/riverhog-ftp-adapter/index.md) — 26 contract elements
  - [riverhog-ftp-adapter-api-client](authorities/riverhog-ftp-adapter-api-client/index.md) — 1 contract element
  - [riverhog-ftp-custody](authorities/riverhog-ftp-custody/index.md) — 1 contract element
  - [riverhog-provenance-linux-contracts](authorities/riverhog-provenance-linux-contracts/index.md) — 6 contract elements
  - [riverhog-provenance-linux-observer](authorities/riverhog-provenance-linux-observer/index.md) — 1 contract element
  - [riverhog-provenance-macos-contracts](authorities/riverhog-provenance-macos-contracts/index.md) — 5 contract elements
  - [riverhog-provenance-macos-observer](authorities/riverhog-provenance-macos-observer/index.md) — 1 contract element
  - [riverhog-provenance-windows-contracts](authorities/riverhog-provenance-windows-contracts/index.md) — 11 contract elements
  - [riverhog-provenance-windows-observer](authorities/riverhog-provenance-windows-observer/index.md) — 1 contract element
  - [riverhog-recover](authorities/riverhog-recover/index.md) — 2 contract elements
  - [riverhog-storage-adapter-aws](authorities/riverhog-storage-adapter-aws/index.md) — 1 contract element
  - [riverhog-storage-adapter-backblaze](authorities/riverhog-storage-adapter-backblaze/index.md) — 1 contract element
  - [riverhog-storage-adapter-filesystem](authorities/riverhog-storage-adapter-filesystem/index.md) — 1 contract element
  - [riverhog-storage-adapter-filesystem-materialize](authorities/riverhog-storage-adapter-filesystem-materialize/index.md) — 1 contract element
  - [riverhog-storage-adapter-s3-support](authorities/riverhog-storage-adapter-s3-support/index.md) — 1 contract element
- [Gogurt](surfaces/references.md#gogurt) — 12 authorities, 37 contract elements
  - [gogurt](authorities/gogurt/index.md) — 22 contract elements
  - [gogurt-core](authorities/gogurt-core/index.md) — 3 contract elements
  - [gogurt-linux-listener-host](authorities/gogurt-linux-listener-host/index.md) — 1 contract element
  - [gogurt-linux-mounted-volume](authorities/gogurt-linux-mounted-volume/index.md) — 1 contract element
  - [gogurt-listener](authorities/gogurt-listener/index.md) — 1 contract element
  - [gogurt-listener-runtime](authorities/gogurt-listener-runtime/index.md) — 3 contract elements
  - [gogurt-macos-listener-host](authorities/gogurt-macos-listener-host/index.md) — 1 contract element
  - [gogurt-macos-mounted-volume](authorities/gogurt-macos-mounted-volume/index.md) — 1 contract element
  - [gogurt-path-volume-support](authorities/gogurt-path-volume-support/index.md) — 1 contract element
  - [gogurt-routes](authorities/gogurt-routes/index.md) — 1 contract element
  - [gogurt-windows-listener-host](authorities/gogurt-windows-listener-host/index.md) — 1 contract element
  - [gogurt-windows-mounted-volume](authorities/gogurt-windows-mounted-volume/index.md) — 1 contract element
- [Mango Fish](surfaces/references.md#mango-fish) — 2 authorities, 8 contract elements
  - [mango-fish](authorities/mango-fish/index.md) — 7 contract elements
  - [mango-fish-cursor](authorities/mango-fish-cursor/index.md) — 1 contract element
- [Piggity](surfaces/references.md#piggity) — 2 authorities, 88 contract elements
  - [piggity](authorities/piggity/index.md) — 87 contract elements
  - [piggity-local](authorities/piggity-local/index.md) — 1 contract element
- [Stove0](surfaces/stove0.md) — 42 authorities, 319 contract elements
  - [Application](surfaces/stove0.md#application) — 7 authorities, 249 contract elements
    - [stove0](authorities/stove0/index.md) — 240 contract elements
    - [stove0-api-client](authorities/stove0-api-client/index.md) — 2 contract elements
    - [stove0-client](authorities/stove0-client/index.md) — 1 contract element
    - [stove0-control](authorities/stove0-control/index.md) — 1 contract element
    - [stove0-operator-contracts](authorities/stove0-operator-contracts/index.md) — 2 contract elements
    - [stove0-protocol](authorities/stove0-protocol/index.md) — 2 contract elements
    - [stove0-server](authorities/stove0-server/index.md) — 1 contract element
  - [Observers](surfaces/stove0.md#observers) — 9 authorities, 22 contract elements
    - [stove0-exiftool-observer](authorities/stove0-exiftool-observer/index.md) — 1 contract element
    - [stove0-ffprobe-sampling-observer](authorities/stove0-ffprobe-sampling-observer/index.md) — 1 contract element
    - [stove0-media-metadata-observer-contracts](authorities/stove0-media-metadata-observer-contracts/index.md) — 1 contract element
    - [stove0-media-sampling-observer-contracts](authorities/stove0-media-sampling-observer-contracts/index.md) — 1 contract element
    - [stove0-observer-client](authorities/stove0-observer-client/index.md) — 3 contract elements
    - [stove0-observer-conformance](authorities/stove0-observer-conformance/index.md) — 1 contract element
    - [stove0-observer-protocol](authorities/stove0-observer-protocol/index.md) — 3 contract elements
    - [stove0-observer-schemas](authorities/stove0-observer-schemas/index.md) — 1 contract element
    - [stove0-observer-support](authorities/stove0-observer-support/index.md) — 10 contract elements
  - [Targets](surfaces/stove0.md#targets) — 10 authorities, 23 contract elements
    - [stove0-media-archive-target-contracts](authorities/stove0-media-archive-target-contracts/index.md) — 1 contract element
    - [stove0-media-archive-target-support](authorities/stove0-media-archive-target-support/index.md) — 1 contract element
    - [stove0-nvenc-av1-opus-target](authorities/stove0-nvenc-av1-opus-target/index.md) — 1 contract element
    - [stove0-opus-target](authorities/stove0-opus-target/index.md) — 1 contract element
    - [stove0-target-client](authorities/stove0-target-client/index.md) — 2 contract elements
    - [stove0-target-conformance](authorities/stove0-target-conformance/index.md) — 1 contract element
    - [stove0-target-jobs](authorities/stove0-target-jobs/index.md) — 1 contract element
    - [stove0-target-protocol](authorities/stove0-target-protocol/index.md) — 3 contract elements
    - [stove0-target-schemas](authorities/stove0-target-schemas/index.md) — 1 contract element
    - [stove0-target-support](authorities/stove0-target-support/index.md) — 11 contract elements
  - [Review](surfaces/stove0.md#review) — 14 authorities, 22 contract elements
    - [stove0-nvenc-av1-opus-review-sampler](authorities/stove0-nvenc-av1-opus-review-sampler/index.md) — 1 contract element
    - [stove0-opus-review-sampler](authorities/stove0-opus-review-sampler/index.md) — 1 contract element
    - [stove0-review-materialize-target](authorities/stove0-review-materialize-target/index.md) — 1 contract element
    - [stove0-review-planning](authorities/stove0-review-planning/index.md) — 2 contract elements
    - [stove0-review-rclone-effect-target](authorities/stove0-review-rclone-effect-target/index.md) — 1 contract element
    - [stove0-review-sampler-client](authorities/stove0-review-sampler-client/index.md) — 1 contract element
    - [stove0-review-sampler-conformance](authorities/stove0-review-sampler-conformance/index.md) — 1 contract element
    - [stove0-review-sampler-protocol](authorities/stove0-review-sampler-protocol/index.md) — 2 contract elements
    - [stove0-review-sampler-schemas](authorities/stove0-review-sampler-schemas/index.md) — 1 contract element
    - [stove0-review-sampler-support](authorities/stove0-review-sampler-support/index.md) — 7 contract elements
    - [stove0-review-target](authorities/stove0-review-target/index.md) — 1 contract element
    - [stove0-review-target-contracts](authorities/stove0-review-target-contracts/index.md) — 1 contract element
    - [stove0-review-target-sampler](authorities/stove0-review-target-sampler/index.md) — 1 contract element
    - [stove0-review-target-support](authorities/stove0-review-target-support/index.md) — 1 contract element
  - [Recipes](surfaces/stove0.md#recipes) — 2 authorities, 3 contract elements
    - [stove0-recipe-config](authorities/stove0-recipe-config/index.md) — 2 contract elements
    - [stove0-recipes](authorities/stove0-recipes/index.md) — 1 contract element

### [Cross-cutting v1 authorities](surfaces/cross-cutting.md)

- [configuration](authorities/configuration/index.md) — 120 contract elements
- [durable-state](authorities/durable-state/index.md) — 1 contract element
- [extent-contract](authorities/extent-contract/index.md) — 15 contract elements
- [release](authorities/release/index.md) — 12 contract elements
- [repository](authorities/repository/index.md) — 5 contract elements


## Contract-wide policies

[Review the normative policies that govern the contract.](policies/index.md)

## Freeze evidence

[Verify completeness, exclusions, ownership, identities, and proof.](evidence/index.md)
