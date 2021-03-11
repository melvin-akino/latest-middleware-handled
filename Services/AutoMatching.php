<?php

use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;
use Workers\{
    // MatchEvent,
    MatchEventMarket,
};

require_once __DIR__ . '/../Bootstrap.php';

function preProcess()
{
    global $dbPool;

    $connection = $dbPool->borrow();

    PreProcess::init($connection);
    PreProcess::loadEnabledProviders();
    PreProcess::loadEnabledSports();
    PreProcess::loadMaintenance();
    PreProcess::loadEnabledProviderAccounts();

    $dbPool->return($connection);
}

$dbPool = null;
makeProcess();

Co\run(function () use ($queue, $activeProcesses) {
    global $dbPool;
    global $swooleTable;

    $dbPool = databaseConnectionPool();

    $dbPool->init();
    defer(function () use ($dbPool) {
        logger('info', 'app', "Closing connection pool");
        echo "Closing connection pool\n";
        $dbPool->close();
    });

    preProcess();

    // go(function () use ($dbPool, $swooleTable) {
    //     MatchEvent::handle($dbPool, $swooleTable);
    // });

    go(function () use ($dbPool, $swooleTable) {
        MatchEventMarket::handle($dbPool, $swooleTable);
    });

});