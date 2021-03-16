<?php

use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;
use Workers\{
    MatchEvent,
    MatchEventMarket,
};

require_once __DIR__ . '/../Bootstrap.php';

$dbPool = null;
Co\run(function () use ($queue) {
    global $dbPool;
    global $swooleTable;

    $dbPool = databaseConnectionPool();

    $dbPool->init();
    defer(function () use ($dbPool) {
        logger('info', 'app', "Closing connection pool");
        echo "Closing connection pool\n";
        $dbPool->close();
    });


    go(function () use ($dbPool, $swooleTable) {
        MatchEvent::handle($dbPool, $swooleTable);
    });

    go(function () use ($dbPool, $swooleTable) {
        MatchEventMarket::handle($dbPool, $swooleTable);
    });

});