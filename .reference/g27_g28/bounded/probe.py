"""Work counts for a synthetic SQLite pinned-directory model, not product targets."""
from __future__ import annotations

import json
from pathlib import Path
import platform
import sqlite3
import tempfile

from model import Store


def measure(n: int, budget: int = 17) -> dict[str, object]:
    with tempfile.TemporaryDirectory() as folder:
        s=Store(Path(folder)/'db')
        max_path=0
        for k in range(n):
            before=s.counters['nodes_created'];s.put(k,f'value-{k}')
            max_path=max(max_path,s.counters['nodes_created']-before)
        root=s.start('query')
        n_before=s.counters['nodes_read']; tx_before=s.counters['transactions']
        units=visits=high=0
        while True:
            epoch=s.claim('query',now=units)
            t=s.ticket('query',epoch,now=units)
            unit=s.compute(t,budget=budget)
            visits+=unit.visits;high=max(high,unit.visits)
            # Every quantum witnesses a distinct committed current root.
            s.put(n+1,f'concurrent-{units}')
            state=s.commit(unit,now=units)
            units+=1
            if state=='READY':break
        rows=[];after=-1;pages=0
        while True:
            values,done=s.page('query',now=0,after=after,size=31)
            rows.extend(values);pages+=1
            if done:break
            after=values[-1][0]
        assert rows==[(k,f'value-{k}') for k in range(n)]
        assert s.db.execute("SELECT root FROM query WHERE id='query'").fetchone()[0]==root
        result={
            'members':n, 'key_width':s.width, 'quantum_budget':budget,
            'admission_node_reads':s.counters['admission_node_reads'],
            'query_node_visits':visits,'query_nodes_bound_2N_plus_w':2*n+s.width,
            'max_unit_node_visits':high,'committed_quanta':units,
            'concurrent_root_changes':units,'whole_query_restarts':0,
            'max_new_nodes_one_write':max_path,'writer_bound_w_plus_1':s.width+1,
            'max_frontier_stack':s.max_stack,'returned_rows':len(rows),'pages':pages,
            'no_content_or_order_difference':True,
            'all_sql_transactions_including_concurrent_writer':s.counters['transactions']-tx_before,
            'all_nodes_read_including_concurrent_writer':s.counters['nodes_read']-n_before,
        }
        s.close();return result


if __name__=='__main__':
    print(json.dumps({'kind':'diagnostic-model-only','python':platform.python_version(),
                      'sqlite':sqlite3.sqlite_version,'measurements':[measure(n) for n in [128,512,2048]]},indent=2))
