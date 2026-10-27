"""Immutable DDL for the current prepared-primary byte-store baseline."""

# ruff: noqa: E501

SQLITE_DDL: tuple[str, ...] = (
    "\nCREATE TABLE primary_format (\n\tid INTEGER NOT NULL, \n\tallocation BLOB NOT NULL, \n\tgeometry TEXT NOT NULL, \n\tbarrier INTEGER NOT NULL, \n\tPRIMARY KEY (id), \n\tCONSTRAINT ck_primary_format_singleton CHECK (id = 1), \n\tCONSTRAINT ck_primary_format_barrier CHECK (barrier >= 0)\n)\n\n",
    "\nCREATE TABLE primary_namespaces (\n\tid INTEGER NOT NULL, \n\tparent INTEGER, \n\tname TEXT NOT NULL, \n\tPRIMARY KEY (id), \n\tCONSTRAINT uq_primary_namespace_name UNIQUE (parent, name), \n\tFOREIGN KEY(parent) REFERENCES primary_namespaces (id)\n)\n\n",
    "\nCREATE TABLE primary_controls (\n\tnamespace INTEGER NOT NULL, \n\tname TEXT NOT NULL, \n\tbody BLOB NOT NULL, \n\tPRIMARY KEY (namespace, name), \n\tFOREIGN KEY(namespace) REFERENCES primary_namespaces (id)\n)\n\n",
    "\nCREATE TABLE primary_objects (\n\tgeneration TEXT NOT NULL, \n\tnamespace INTEGER NOT NULL, \n\tname TEXT NOT NULL, \n\tbytes INTEGER NOT NULL, \n\tsha256 TEXT NOT NULL, \n\tcomplete INTEGER NOT NULL, \n\tPRIMARY KEY (generation), \n\tCONSTRAINT ck_primary_object_bytes CHECK (bytes >= 0), \n\tCONSTRAINT ck_primary_object_complete CHECK (complete IN (0, 1)), \n\tFOREIGN KEY(namespace) REFERENCES primary_namespaces (id)\n)\n\n",
    "\nCREATE TABLE primary_chunks (\n\tgeneration TEXT NOT NULL, \n\tordinal INTEGER NOT NULL, \n\tbody BLOB NOT NULL, \n\tPRIMARY KEY (generation, ordinal), \n\tCONSTRAINT ck_primary_chunk_ordinal CHECK (ordinal >= 0), \n\tCONSTRAINT ck_primary_chunk_bytes CHECK (length(body) BETWEEN 1 AND 4096), \n\tFOREIGN KEY(generation) REFERENCES primary_objects (generation)\n)\n\n",
    "\nCREATE TABLE primary_names (\n\tnamespace INTEGER NOT NULL, \n\tname TEXT NOT NULL, \n\tgeneration TEXT NOT NULL, \n\tPRIMARY KEY (namespace, name), \n\tCONSTRAINT uq_primary_name_generation UNIQUE (generation), \n\tFOREIGN KEY(namespace) REFERENCES primary_namespaces (id), \n\tFOREIGN KEY(generation) REFERENCES primary_objects (generation)\n)\n\n",
    "\nCREATE TABLE primary_operation (\n\tid INTEGER NOT NULL, \n\tgeneration TEXT NOT NULL, \n\tcursor INTEGER NOT NULL, \n\tphase TEXT NOT NULL, \n\tPRIMARY KEY (id), \n\tCONSTRAINT ck_primary_operation_phase CHECK (phase IN ('writing', 'cleanup')), \n\tCONSTRAINT ck_primary_operation_singleton CHECK (id = 1), \n\tCONSTRAINT ck_primary_operation_cursor CHECK (cursor >= 0), \n\tFOREIGN KEY(generation) REFERENCES primary_objects (generation)\n)\n\n",
)
