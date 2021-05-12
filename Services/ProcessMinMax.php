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
    $primaryProviderName = strtolower($primaryProvider['value']);
    $dbPool->return($connection);
    
    /**
     * Co-Routine Asynchronous Worker for handling
     * User watchlist and Major Leagues in the following game schedules [inplay, today, early].
     * 
     * NEW ENV VARIABLES
     * KAFKA_MINMAXHIGH=minmax-high_req
     *   MINMAX_FREQUENCY_INPLAY=10
     *   MINMAX_FREQUENCY_TODAY=15
     *   MINMAX_FREQUENCY_EARLY=20
     */
    go(function () use ($dbPool, $providers, $primaryProviderName) {
        ProcessHighFrequencyMinMax::handle($dbPool, $providers, $primaryProviderName, 'inplay', getenv('MINMAX_FREQUENCY_INPLAY'));
    });

    go(function () use ($dbPool, $providers, $primaryProviderName) {
        ProcessHighFrequencyMinMax::handle($dbPool, $providers, $primaryProviderName, 'today', getenv('MINMAX_FREQUENCY_TODAY'));
    });

    go(function () use ($dbPool, $providers, $primaryProviderName) {
        ProcessHighFrequencyMinMax::handle($dbPool, $providers, $primaryProviderName, 'early', getenv('MINMAX_FREQUENCY_EARLY'));
    });

});