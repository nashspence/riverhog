# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [Riverhog #991](https://github.com/nashspence/riverhog/issues/991). Request: [neutral history verification](https://github.com/nashspence/riverhog/issues/991#issuecomment-7233007754). Convention: [#903](https://github.com/nashspence/riverhog/issues/903).

Audited base: `main` @ `60612eded1e0e4e5188c28a84697435eb969fad0`. The requested `8d278b6f` neutral and recovery source is byte-identical to this base. Recovery #1009 and activation #1010 are now merged.

Produced and published by: OpenAI ChatGPT, GPT-6 Astra Pro, with collaborating agents using the inherited model. No hidden configuration or effort level is asserted. Requested by: the repository maintainer for the Riverhog integration agent.

## First integration step

Review [DESIGN.md](DESIGN.md), especially the exact lifetime of reusable set facts and the fresh-export fingerprint requirement. Reconcile current main, then apply the **whole** [focused patch](proposed/neutral-history.patch) in an isolated integration checkout and run the focused commands in [VALIDATION.md](VALIDATION.md). The fingerprint and rollback/receiving witnesses are part of the same change.

The proposal changes two neutral production owners. It uses the existing closure worklist to consume each verified set once per history/extent, removes per-import complete-set rereads, and preserves fresh proof/descriptor/primary-journal checks. Existing object inventory gains only an exact digest and byte extent; no payload cache, extra database, process pool or dependency is introduced. Whole-resolution rollback prevents late inherited failures from exposing partial membership; savepoints add no per-member commit.

## Reproduced evidence

- 270 focused maintained cases passed across the recorded sources. The 173-case broader run preceded one final post-read open check and a docstring clarification; all 25 new closure cases, two live-fence cases and the 40-observation probe then passed on final bytes. The detailed case inventory and source relationship are in [validation evidence](evidence/validation.json).
- 40 final comparison observations passed: 34 neutral-owner observations and 6 actual encrypted recovery observations. For 1/2/4 members, official decryptions fell **39/66/120 → 25/38/64**, with exact recovered payloads, history objects, journals and complete receipts.
- The encrypted fixture uses KDF `log_n=1`; the counts establish avoided work, not production wall-clock savings or budget qualification.
- A same-authority changed-page counterexample led to exact fresh-export verification. Original source/cursor/connection receiving and interrupted attempts are covered.

Read [EVIDENCE.md](EVIDENCE.md) for added SQL/hash work and remaining growth, and [SOURCES.json](SOURCES.json) for exact identities. The product tree on this reference branch is unchanged from the audited base; proposed edits exist only inside `.reference/`.

This is a new finite slice, not a supersession of the recovery or wake handoffs. It creates no hold and changes no archive format, KDF, full-history contract, shared allowance/profile authority, or #1007 acceptance scope. Full current-source integration CI, installed/platform qualification and a matched production diagnostic remain unclaimed. Record the actual finite disposition in #991 through the normal integration rail; do not merge this reference branch.
