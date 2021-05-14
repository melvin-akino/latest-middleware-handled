<?php

use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;
use Workers\ProcessHighFrequencyMinMax;
use Models\{Provider,SystemConfiguration};

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

    $primaryProviderResult = SystemConfiguration::getPrimaryProvider($connection);
    $primaryProvider       = $connection->fetchArray($primaryProviderResult);
    $primaryProviderName   = strtolower($primaryProvider['value']);
    $dbPool->return($connection);
    
    /**
     * Co-Routine Asynchronous Worker for handling
     * User watchlist and Major Leagues in the following game schedules [inplay, today, early].
     * 
     * NEW ENV VARIABLES
     *   KAFKA_MINMAXHIGH=minmax-high_req
     *   MINMAX_FREQUENCY_INPLAY=5
     *   MINMAX_FREQUENCY_TODAY=10
     *   MINMAX_FREQUENCY_EARLY=15
     */

    function getInPlayData($timer, $dbPool, $providers, $primaryProviderName) {
        ProcessHighFrequencyMinMax::handle($dbPool, $providers, $primaryProviderName, 'inplay');
    }
    function getTodayData($timer, $dbPool, $providers, $primaryProviderName) {
        ProcessHighFrequencyMinMax::handle($dbPool, $providers, $primaryProviderName, 'today');
    }
    function getEarlyData($timer, $dbPool, $providers, $primaryProviderName) {
        ProcessHighFrequencyMinMax::handle($dbPool, $providers, $primaryProviderName, 'early');
    }

    Swoole\Timer::tick((getenv('MINMAX_FREQUENCY_INPLAY') * 1000), "getInPlayData", $dbPool, $providers, $primaryProviderName);
    Swoole\Timer::tick((getenv('MINMAX_FREQUENCY_TODAY') * 1000), "getTodayData", $dbPool, $providers, $primaryProviderName);
    Swoole\Timer::tick((getenv('MINMAX_FREQUENCY_EARLY') * 1000), "getEarlyData", $dbPool, $providers, $primaryProviderName);
});