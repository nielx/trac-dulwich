from trac.db import Table, Column

name = 'dulwich'
version = 1
tables = [
    Table('dulwich_objects', key=('repos', 'sha', 'path', 'commit_id'))[
        Column('repos', type="int"),
        Column('sha', key_size=40),
        Column('path'),
        Column('mode', type='integer'),
        Column('commit_id', key_size=40),
    ],
    Table('dulwich_heads', key=('repos', 'head'))[
        Column('repos', type="int"),
        Column('head', key_size=40),
    ],
]

migrations = []