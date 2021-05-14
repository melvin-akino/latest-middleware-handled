<?php
use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;
use Workers\ProcessHighFrequencyMinMax;
use Models\{Provider,SystemConfiguration};
use Models\{
    EventMarket,
    League,
    UserWatchlist
};
use Carbon\Carbon;
use Co\System;
use Ramsey\Uuid\Uuid;
require_once __DIR__ . '/../Bootstrap.php';
function process($connection, $providers, $primaryProviderName, $schedule) {
    logger('info', 'minmax', "[".strtoupper($schedule)."] Processing event markets...");
    $providerId = array_search($primaryProviderName, $providers);
    try {
        $userWatchlists  = UserWatchlist::getUserWatchlists($connection, $providerId, $schedule);
        $majorLeagues  = League::getMajorLeagues($connection, $providerId, $schedule);
        $events = array_unique(array_merge($userWatchlists, $majorLeagues));
        if (!empty($events)) {
            $masterEventIds = implode(",",$events);
            $eventMarkets = EventMarket::getMarketsByMasterEventIds($connection,$masterEventIds,$providerId);
            if ($eventMarkets) {
                $eventMarketArray = $connection->fetchAll($eventMarkets);
                if ($eventMarketArray) {
                    foreach($eventMarketArray as $market) {
                        //Push to Kafka
                        $requestId = (string) Uuid::uuid4();
                        $requestTs = getMilliseconds();
                        //Generate kafka json payload here
                        $payload = [
                            'request_uid'    => $requestId,
                            'request_ts'    => $requestTs,
                            'command'       => 'minmax',
                            'sub_command'   => 'scrape',
                            'data' => [
                                'provider'      => $providers[$market['provider_id']],
                                'sport'         => (string) $market['sport_id'],
                                'schedule'      => $market['game_schedule'],
                                'event_id'      => (string) $market['event_id'],
                                'market_id'      => (string) $market['bet_identifier'],
                                'odds'          => (string) $market['odds'],
                                'memUID'        => $market['mem_uid']
                            ]
                        ];
                        $topic = getenv('KAFKA_MINMAXHIGH', 'minmax-high_req');
                        if (!in_array(getenv('APP_ENV'), ['testing'])) {
                            kafkaPush($topic, $payload, $requestId);
                            logger('info', 'minmax', "[".strtoupper($schedule)."] Pushed this event market mem_uid:".$market['mem_uid']." to kafka");
                        }
                    }
                }
            }
        } else {
            logger('info', 'minmax', "[".strtoupper($schedule)."] There are no event markets to process.");
        }
    } catch (Exception $e) {
        logger('error', 'minmax', "[".strtoupper($schedule)."] Something went wrong during Processing of event markets...", (array) $e);
    }
}
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
     *   MINMAX_FREQUENCY_INPLAY=1
     *   MINMAX_FREQUENCY_TODAY=5
     *   MINMAX_FREQUENCY_EARLY=10
     */
    // function getInPlayData($timer, $dbPool, $providers, $primaryProviderName) {
    //     ProcessHighFrequencyMinMax::handle($dbPool, $providers, $primaryProviderName, 'inplay');
    // }
    // function getTodayData($timer, $dbPool, $providers, $primaryProviderName) {
    //     ProcessHighFrequencyMinMax::handle($dbPool, $providers, $primaryProviderName, 'today');
    // }
    // function getEarlyData($timer, $dbPool, $providers, $primaryProviderName) {
    //     ProcessHighFrequencyMinMax::handle($dbPool, $providers, $primaryProviderName, 'early');
    // }
    // Swoole\Timer::tick((getenv('MINMAX_FREQUENCY_INPLAY') * 1000), "getInPlayData", $dbPool, $providers, $primaryProviderName);
    // Swoole\Timer::tick((getenv('MINMAX_FREQUENCY_TODAY') * 1000), "getTodayData", $dbPool, $providers, $primaryProviderName);
    // Swoole\Timer::tick((getenv('MINMAX_FREQUENCY_EARLY') * 1000), "getEarlyData", $dbPool, $providers, $primaryProviderName);
    // Swoole\Timer::tick(60000, "getTodayData", [$providers, $primaryProviderName]);
    go(function () use ($dbPool, $providers, $primaryProviderName) {
        while (true) {
            try {
                $connection = $dbPool->borrow();
                process($connection, $providers, $primaryProviderName, 'inplay');
                $dbPool->return($connection);
            } catch (Exception $e) {
                echo $e->getMessage();
            }
            Co::sleep((getenv('MINMAX_FREQUENCY_INPLAY') * 1000));
        }
    });
    go(function () use ($dbPool, $providers, $primaryProviderName) {
        while (true) {
            try {
                $connection = $dbPool->borrow();
                process($connection, $providers, $primaryProviderName, 'today');
                $dbPool->return($connection);
            } catch (Exception $e) {
                echo $e->getMessage();
            }
            Co::sleep((getenv('MINMAX_FREQUENCY_TODAY') * 1000));
        }
    });
    go(function () use ($dbPool, $providers, $primaryProviderName) {
        while (true) {
            try {
                $connection = $dbPool->borrow();
                process($connection, $providers, $primaryProviderName, 'early');
                $dbPool->return($connection);
            } catch (Exception $e) {
                echo $e->getMessage();
            }
            Co::sleep((getenv('MINMAX_FREQUENCY_EARLY') * 1000));
        }
    });
});