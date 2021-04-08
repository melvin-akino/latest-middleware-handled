<?php

use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;
use Workers\{
    MatchLeague,
    MatchTeam,
    MatchEvent,
    MatchEventMarket,
    UnmatchedData
};

require_once __DIR__ . '/../Bootstrap.php';

function preProcess()
{
    global $dbPool;

    $connection = $dbPool->borrow();

    PreProcess::init($connection);
    PreProcess::loadEnabledProviders();
    PreProcess::loadSystemConfig();
    PreProcess::loadUnmatchedData();
    PreProcess::loadMatchedLeaguesData();
    PreProcess::loadMatchedTeamsData();
    PreProcess::loadEventMarkets();
    PreProcess::loadMatchedEventsData();

    $dbPool->return($connection);
}

function resetProcess()
{
    global $dbPool;

    $connection = $dbPool->borrow();

    PreProcess::loadEventMarkets();
    PreProcess::loadUnmatchedData();

    $dbPool->return($connection);
}

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

    preProcess();

    Swoole\Timer::tick(10000, "resetProcess");

    /**
     * Co-Routine Asynchronous Worker for handling
     * Leagues Auto-matching.
     */
    go(function () use ($dbPool, $swooleTable) {
        MatchLeague::handle($dbPool, $swooleTable);
    });

    go(function () use ($dbPool, $swooleTable) {
        MatchTeam::handle($dbPool, $swooleTable);
    });

    go(function () use ($dbPool, $swooleTable) {
        MatchEvent::handle($dbPool, $swooleTable);
    });

    go(function () use ($dbPool, $swooleTable) {
        MatchEventMarket::handle($dbPool, $swooleTable);
    });

    go(function () use ($dbPool, $swooleTable) {
        UnmatchedData::handle($dbPool, $swooleTable);
    });

});