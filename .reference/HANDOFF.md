# Non-authoritative external reference

Owner: [#976](https://github.com/nashspence/riverhog/issues/976)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Audited base: `main` @ `192270f3531bea0e6b1d65b0b5514229007e538a`
Base tree: `9e564d8044fe0d6a2d3b17e4f41f0c87c435840c`
Branch: `reference/external/976-stove0-tag-selection`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer, in the issue-design conversation
Published by: OpenAI ChatGPT through the connected GitHub interface
Git author/committer metadata identifies publication, not a different producer.

## Start here

Read #976 for the recommended design and acceptance. First reproduce the focused
witness below, then introduce the common protocol tag grammar and client evaluator
with actual discovery/Stove0 parity tests on current main. Use
[tag-selection/INTEGRATION.md](tag-selection/INTEGRATION.md) only for the short owner
map and the prototype's limits. Production integration is intentionally left to the
integration agent; neither prototype signatures nor checkpoint layout are mandated.

```sh
python -m unittest discover -s .reference/tag-selection -p 'test_*.py' -v
python -m compileall -q .reference/tag-selection
```

## Source and evidence

This initial reference adds exactly four files, all under `.reference/`. Product
paths match the audited base exactly. There is no production patch or imported
historical reference. Publication reconstructs the tested tree through GitHub Git
objects; the owning issue records the actual published SHA as the durable identity.
No producer-side local commit SHA is claimed as imported.

Performed on Python 3.13.5: 19 reference tests passed; compilation and staged
whitespace checks passed. One test checks all 4,096 combinations of three-tag
membership and all/any/none clauses; another completes 768 distinct probes in
one-contact steps with simulated handoff between steps. Maximum ASCII/Unicode tag,
malformed evidence, scope-fence and interrupted-contact cases are included. Tests
execute the audited source's pure tag validator and inspect discovery's clause
bounds/defaults. They do not run the full product or its discovery SQL.

Not performed: locked repository lint/unit/distribution/build/linux-qualification,
HTTP/service integration, SQLite/PostgreSQL transactions, real restart/concurrency,
installed API/CLI/configuration, canonical-identity or transport-size qualification,
production growth witnesses, or Actions qualification. Full-checkpoint deepcopy is
only a functional restart simulation, not a recommended persistence algorithm or
proof of bounded storage/serialization cost. Scope values are opaque test fences,
not independently authenticated authority. See the owner issue for required proof.

This material is non-authoritative. Its request, publication and passing tests do
not approve integration, extend a contract or complete the issue. The owning issue,
subsequent authorized decisions and the then-current normal integration rail
control. Reconcile rather than apply mechanically, and record finite consumption
in the owning issue. No PR, auto-close or native Development association is intended
or expected; the issue's branch URL and exact reference SHA are the handoff record.
