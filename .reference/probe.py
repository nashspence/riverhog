"""Executable reference vectors, deliberately independent of Riverhog dependencies."""
from __future__ import annotations

import json
import os
import platform
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path

from quiescence_model import ClosedEffect, Conflict, Ledger, Ticket, canonical

HERE = Path(__file__).resolve().parent


class Probe(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'catalog.sqlite3'
        self.owner = Ledger(self.path)
        self.owner.create('incarnation/collection/prefix', rows=17)
        self.scope = 'incarnation/collection/prefix'

    def tearDown(self):
        self.owner.close()
        self.tmp.cleanup()

    def ticket(self, kind='begin', object_id='v1'):
        return self.owner.prepare(self.scope, 'worker-1',
                                  {'kind': kind, 'object': object_id, 'plan': 'original-plan',
                                   'crypto': 'original-age-state', 'part': 1})

    def closed(self, ticket, outcome='begun'):
        return ClosedEffect(ticket, outcome, canonical({'exact': 'original', 'session': 'session-1'}))

    def reopen(self):
        self.owner.close()
        self.owner = Ledger(self.path)

    def drain_catalog(self, key):
        calls = 0
        while self.owner.outcome(key) is None:
            self.assertLessEqual(self.owner.cleanup(key, limit=3), 3)
            calls += 1
            self.assertLess(calls, 20)

    def test_counterexample_untracked_begin_can_outlive_deleted_owner(self):
        # Isolated negative model of the ordering reported by integration, not its fixture.
        reached, release = threading.Event(), threading.Event()
        remote, owner = [], {'present': True}
        def worker():
            reached.set()
            if not release.wait(5):
                return
            remote.append('late-session')
        thread = threading.Thread(target=worker)
        thread.start()
        self.assertTrue(reached.wait(5))
        owner['present'] = False
        release.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(remote, ['late-session'])
        self.assertFalse(owner['present'])

    def test_closed_gate_rejects_prepared_but_unarmed_call(self):
        t = self.ticket()
        key = self.owner.select_terminal(self.scope, 'cancel')
        with self.assertRaises(Conflict):
            self.owner.arm(t)
        self.assertEqual(self.owner.adjudicate(key), 'provider-cleanup')

    def test_armed_late_begin_drains_without_authorizing_next_write(self):
        t = self.ticket()
        self.owner.arm(t)
        reached, release, returned = threading.Event(), threading.Event(), threading.Event()
        remote = []
        errors = []
        def worker():
            local = Ledger(self.path)
            try:
                reached.set()
                if not release.wait(5):
                    raise AssertionError('event deadline')
                remote.append('session-1')
                local.settle(self.closed(t))
                returned.set()
            except BaseException as exc:
                errors.append(repr(exc))
            finally:
                local.close()
        thread = threading.Thread(target=worker)
        thread.start()
        self.assertTrue(reached.wait(5))
        key = self.owner.select_terminal(self.scope, 'cancel')
        self.assertEqual(self.owner.adjudicate(key), 'draining')
        self.assertTrue(self.owner.construction(self.scope)['owner_present'])
        release.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(returned.is_set())
        with self.assertRaises(Conflict):
            self.ticket('part')
        self.assertEqual(self.owner.adjudicate(key), 'provider-cleanup')
        remote.remove('session-1')  # Synchronous model provider: no further effects.
        self.owner.confirm_provider_empty(key)
        self.drain_catalog(key)
        self.assertEqual(remote, [])
        self.assertFalse(self.owner.construction(self.scope)['owner_present'])

    def test_late_part_does_not_admit_completion(self):
        t = self.ticket('part')
        self.owner.arm(t)
        key = self.owner.select_terminal(self.scope, 'cancel')
        self.assertEqual(self.owner.adjudicate(key), 'draining')
        self.owner.settle(self.closed(t, 'part'))
        with self.assertRaises(Conflict):
            self.ticket('complete')
        self.assertEqual(self.owner.adjudicate(key), 'provider-cleanup')

    def test_late_seal_refuses_cancel_and_retains_owner(self):
        t = self.ticket('complete')
        self.owner.arm(t)
        key = self.owner.select_terminal(self.scope, 'cancel')
        self.owner.settle(self.closed(t, 'sealed'))
        self.assertEqual(self.owner.adjudicate(key), 'refused-sealed')
        with self.assertRaises(Conflict):
            self.owner.confirm_provider_empty(key)
        self.assertTrue(self.owner.construction(self.scope)['owner_present'])
        self.assertEqual(self.owner.db.execute('SELECT COUNT(*) FROM retained').fetchone()[0], 17)

    def test_late_seal_does_not_expand_selected_discard_snapshot(self):
        t = self.ticket('complete')
        self.owner.arm(t)
        key = self.owner.select_terminal(self.scope, 'discard', challenge='exact-original')
        self.owner.settle(self.closed(t, 'sealed'))
        self.assertEqual(self.owner.adjudicate(key), 'needs-reconfirmation')
        self.assertIsNone(self.owner.outcome(key))

    def test_negative_observation_and_timeout_do_not_close_begin(self):
        t = self.ticket()
        self.owner.arm(t)
        key = self.owner.select_terminal(self.scope, 'cancel')
        for _ in range(5):
            self.owner.observe(t)
        self.reopen()
        self.assertEqual(self.owner.adjudicate(key), 'draining')
        self.assertEqual(self.owner.pending(self.scope), [t.sequence])
        # The remote effect may still happen after all five negative observations.
        self.owner.settle(self.closed(t))
        self.assertEqual(self.owner.adjudicate(key), 'provider-cleanup')

    def test_positive_seal_observation_preserves_custody_not_execution_closure(self):
        t = self.ticket('complete')
        self.owner.arm(t)
        key = self.owner.select_terminal(self.scope, 'cancel')
        self.owner.observe(t, sealed=True)
        self.assertEqual(self.owner.adjudicate(key), 'draining')
        self.owner.settle(self.closed(t, 'sealed'))
        self.assertEqual(self.owner.adjudicate(key), 'refused-sealed')

    def test_checkpoint_failure_retains_armed_effect_then_exact_report_replays(self):
        t = self.ticket()
        self.owner.arm(t)
        e = self.closed(t)
        with self.assertRaises(OSError):
            self.owner.settle(e, fail_before_commit=True)
        self.reopen()
        self.assertEqual(self.owner.pending(self.scope), [t.sequence])
        self.owner.settle(e)
        self.owner.settle(e)
        self.assertEqual(self.owner.pending(self.scope), [])

    def test_lost_checkpoint_commit_response_is_idempotent(self):
        t = self.ticket('part')
        self.owner.arm(t)
        e = self.closed(t, 'part')
        self.owner.settle(e)
        self.reopen()
        self.owner.settle(e)
        with self.assertRaises(Conflict):
            self.owner.settle(replace(e, result='different'))

    def test_ready_replay_cannot_rearm_previously_used_ticket(self):
        t = self.ticket()
        self.owner.arm(t)
        self.owner.settle(self.closed(t))
        with self.assertRaises(Conflict):
            self.owner.arm(t)

    def test_stale_worker_or_changed_plan_cannot_report(self):
        t = self.ticket()
        self.owner.arm(t)
        for modified in (replace(t, worker='other'), replace(t, epoch=t.epoch+1),
                         replace(t, descriptor='{}'), replace(t, scope='other')):
            with self.assertRaises(Conflict):
                self.owner.settle(self.closed(modified))
        self.assertEqual(self.owner.pending(self.scope), [t.sequence])

    def test_initial_expired_challenge_and_live_claim_reject(self):
        with self.assertRaises(Conflict):
            self.owner.select_terminal(self.scope, 'discard', challenge='old', now=101)
        self.owner.db.execute('UPDATE construction SET claim_live=1 WHERE scope=?', (self.scope,))
        with self.assertRaises(Conflict):
            self.owner.select_terminal(self.scope, 'discard', challenge='new')
        self.assertEqual(self.owner.construction(self.scope)['gate'], 'open')

    def test_selected_exact_challenge_survives_expiry_restart_and_partial_cleanup(self):
        key = self.owner.select_terminal(self.scope, 'discard', challenge='original', now=99)
        self.assertEqual(self.owner.adjudicate(key), 'provider-cleanup')
        self.owner.confirm_provider_empty(key)
        self.assertEqual(self.owner.cleanup(key, 3), 3)
        self.reopen()
        self.assertEqual(key, self.owner.select_terminal(self.scope, 'discard', challenge='original', now=500))
        self.drain_catalog(key)
        original = self.owner.outcome(key)
        self.reopen()
        self.assertEqual(key, self.owner.select_terminal(self.scope, 'discard', challenge='original', now=900))
        self.assertEqual(self.owner.outcome(key), original)
        self.assertIsNotNone(original)

    def test_changed_request_cannot_replace_partial_cleanup_authority(self):
        key = self.owner.select_terminal(self.scope, 'discard', challenge='original')
        self.owner.adjudicate(key)
        self.owner.confirm_provider_empty(key)
        self.owner.cleanup(key, 3)
        for kind, challenge, principal in [('discard','other','authorized-owner'),
                                          ('cancel','','authorized-owner'),
                                          ('discard','original','other')]:
            with self.assertRaises(Conflict):
                self.owner.select_terminal(self.scope, kind, challenge=challenge, principal=principal)

    def test_cleanup_transaction_rolls_back_and_never_reports_partial_done(self):
        key = self.owner.select_terminal(self.scope, 'cancel')
        self.owner.adjudicate(key)
        self.owner.confirm_provider_empty(key)
        with self.assertRaises(OSError):
            self.owner.cleanup(key, 3, fail=True)
        self.assertEqual(self.owner.db.execute('SELECT COUNT(*) FROM retained').fetchone()[0], 17)
        self.assertIsNone(self.owner.outcome(key))
        self.drain_catalog(key)

    def test_unknown_effect_capacity_is_retained_but_other_construction_progresses(self):
        a, b = self.ticket(object_id='v1'), self.ticket(object_id='v2')
        self.owner.arm(a, maximum_live=1)
        with self.assertRaises(Conflict):
            self.owner.arm(b, maximum_live=1)
        other = Ledger(self.path)
        try:
            other.create('independent')
            t = other.prepare('independent','worker-2',{'kind':'begin'})
            other.arm(t, maximum_live=1)
            other.settle(self.closed(t))
        finally:
            other.close()
        self.assertEqual(self.owner.pending(self.scope), [a.sequence])

    def test_point_pending_uses_index_and_bounded_result_after_completed_backlog(self):
        for n in range(129):
            t = self.ticket(object_id=str(n))
            self.owner.arm(t)
            self.owner.settle(self.closed(t))
        expected=[]
        for n in range(5):
            t=self.ticket(object_id=f'late-{n}')
            self.owner.arm(t)
            expected.append(t.sequence)
        self.reopen()
        self.assertEqual(self.owner.pending(self.scope, 3), expected[:3])
        plan=self.owner.db.execute("EXPLAIN QUERY PLAN SELECT sequence FROM effects WHERE scope=? "
                                   "AND state='armed' ORDER BY sequence LIMIT 3", (self.scope,)).fetchall()
        self.assertTrue(any('unsettled' in row[3] for row in plan))

    def test_concurrent_terminal_requests_have_one_winner(self):
        ready = threading.Barrier(3)
        results=[]
        def choose(kind):
            local=Ledger(self.path)
            try:
                ready.wait(5)
                results.append(('ok',local.select_terminal(self.scope,kind,challenge='challenge')))
            except Conflict:
                results.append(('conflict',kind))
            finally:
                local.close()
        threads=[threading.Thread(target=choose,args=(kind,)) for kind in ('cancel','discard')]
        for t in threads: t.start()
        ready.wait(5)
        for t in threads: t.join(5)
        self.assertEqual(sorted(r[0] for r in results), ['conflict','ok'])

    def test_actual_process_exit_after_arm_does_not_prove_provider_stopped(self):
        t=self.ticket()
        env=dict(os.environ, PYTHONPATH=str(HERE))
        code=('from pathlib import Path\nfrom quiescence_model import Ledger,Ticket\nimport os,sys,json\n'
              'o=Ledger(Path(sys.argv[1]));o.arm(Ticket(**json.loads(sys.argv[2])));os._exit(71)\n')
        process=subprocess.run([sys.executable,'-c',code,str(self.path),json.dumps(t.__dict__)], env=env)
        self.assertEqual(process.returncode,71)
        self.reopen()
        key=self.owner.select_terminal(self.scope,'cancel')
        self.assertEqual(self.owner.adjudicate(key),'draining')
        self.owner.observe(t)  # Even an empty remote snapshot cannot discharge the intent.
        self.assertEqual(self.owner.pending(self.scope),[t.sequence])

    def test_zero_effect_cancel_and_terminal_tombstone_prevent_reinitialization(self):
        key=self.owner.select_terminal(self.scope,'cancel')
        self.assertEqual(self.owner.adjudicate(key),'provider-cleanup')
        self.owner.confirm_provider_empty(key)
        self.drain_catalog(key)
        with self.assertRaises(sqlite3.IntegrityError):
            self.owner.create(self.scope)
        self.assertEqual(key,self.owner.select_terminal(self.scope,'cancel'))

    def test_live_claim_reappearing_before_cleanup_requires_reconfirmation(self):
        key=self.owner.select_terminal(self.scope,'discard',challenge='original')
        self.owner.db.execute('UPDATE construction SET claim_live=1 WHERE scope=?',(self.scope,))
        self.assertEqual(self.owner.adjudicate(key),'needs-reconfirmation')
        with self.assertRaises(Conflict): self.owner.confirm_provider_empty(key)


if __name__ == '__main__':
    started=time.perf_counter()
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Probe))
    receipt={'format':'reference-probe/v1', 'source':'533ac4ee06def4692288c39c9279bf9387525a3e',
             'python':platform.python_version(),'platform':platform.system(),'sqlite':sqlite3.sqlite_version,
             'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),
             'elapsed_seconds':time.perf_counter()-started,
             'scope':'isolated SQLite/effect model; no maintained service or provider execution',
             'events':'event/barrier ordered threads, no timing sleeps; one actual child exit after commit',
             'not_validated':['PostgreSQL','actual HTTP','S3','filesystem adapter','encryption','custody roots','power loss','product growth']}
    (HERE/'PROBE_RESULT.json').write_text(json.dumps(receipt,indent=2)+'\n')
    raise SystemExit(0 if result.wasSuccessful() else 1)
