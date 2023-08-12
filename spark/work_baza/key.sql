CREATE KEYSPACE IF NOT EXISTS Test
    WITH replication = {'class': 'SimpleStrategy',
                        'replication_factor' : 1};
