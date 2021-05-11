<?php

use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;
use Workers\{
    ProcessUserWatchlist,
    ProcessUserSelectedLeague,
    ProcessMajorLeague
};
use Models\Provider;

require_once __DIR__ . '/../Bootstrap.php';

$dbPool = null;
Co\run(function () use ($queue) {
    global $dbPool;

    $dbPool = databaseConnectionPool();

    $dbPool->init();
    defer(function () use ($dbPool) {
        logger('info', 'app', "Closing connection pool");
        echo "Closing connection pool\n";
        $dbPool->close();
    });
    $connection = $dbPool->borrow();
    $providerQuery = Provider::getActiveProviders($connection);
    $providerArray = $connection->fetchAll($providerQuery);
    foreach($providerArray as $provider)
    {
        $providers[$provider['id']] = strtolower($provider['alias']);
    }
    $dbPool->return($connection);
    /**
     * Co-Routine Asynchronous Worker for handling
     * User watchlist.
     */
    go(function () use ($dbPool, $providers) {
        ProcessUserWatchlist::handle($dbPool, $providers);
    });

    go(function () use ($dbPool, $providers) {
        ProcessUserSelectedLeague::handle($dbPool, $providers);
    });

    go(function () use ($dbPool, $providers) {
        ProcessMajorLeague::handle($dbPool, $providers);
    });

});