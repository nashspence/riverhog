"""One explicit entrypoint for the exact-base candidate and its witness corrections."""
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parents[1]
runpy.run_path(str(ROOT / ".reference" / "apply.py"), run_name="__main__")
p = ROOT / "tests/support/async_retirement_growth.py"
s = p.read_text()


def replace(before, after):
    global s
    assert s.count(before) == 1, "unexpected observer source; use exact candidate inputs"
    s = s.replace(before, after)


replace(
    "ordinary_charged=context.reservation.key in self.original.ledger._reservations)",
    "ordinary_charged=self.original.ledger._reservations.get(id(context.reservation)) is context.reservation)",
)
replace(
    """        with pytest.MonkeyPatch.context() as patch:
            operations = [""",
    """        # Carry-in state belongs to preceding evidence, not a new submission.
        for context in tuple(original.registry._contexts.values()):
            charged = original.ledger._reservations.get(id(context.reservation)) is context.reservation
            assert charged
            self.event("carry-in", context=context, ordinary_charged=charged,
                       actor_received=bool(context.actor and context.actor.received),
                       closing=context.closing, closed=context.closed)
            slot = original.pool._slots.get(context.reservation.key)
            if slot is not None:
                assert slot.identity is context and not slot.worker.is_alive()
                slot.worker.join(timeout=0)
                assert not slot.worker.is_alive()
                self.event("carry-in-joined", context=context, worker=slot.worker,
                           actor_received=bool(context.actor and context.actor.received),
                           ordinary_charged=charged)
        with pytest.MonkeyPatch.context() as patch:
            operations = [""",
)
replace(
    """                    result=None
                    try:
                        with execution.observed""",
    """                    prior_workers = {id(slot.identity): slot.worker for slot in pool._slots.values()}
                    result=None
                    try:
                        with execution.observed""",
)
replace(
    '                            self.event("physical-received",context=outcome.identity)',
    '                            self.event("physical-received",context=outcome.identity,\n'
    '                                       worker=prior_workers[id(outcome.identity)])',
)
replace(
    '                        self.event("ordinary-withdrawn",context=context)',
    """                        assert context.closed
                        self.event("ordinary-withdrawn",context=context,
                                   actor_received=bool(context.actor and context.actor.received),
                                   close_confirmed=context.closed,
                                   ordinary_charged=ledger._reservations.get(id(arg)) is arg)""",
)
p.write_text(s)

# Normalize the generated owner's trailing blank line; no executable change.
p = ROOT / "riverhog/src/riverhog_core/services/upload_journal_validation_retirement.py"
p.write_text(p.read_text().rstrip() + "\n")
