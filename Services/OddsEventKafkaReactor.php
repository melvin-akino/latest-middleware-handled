<?php

use App\Facades\SwooleStats;
use RdKafka\{KafkaConsumer, Conf, Consumer, TopicConf};
use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;
use Workers\{ProcessOdds, ProcessEvent};

require_once __DIR__ . '/../Bootstrap.php';

function makeConsumer()
{
    // LOW LEVEL CONSUMER
    $topics = [
        getenv('KAFKA_SCRAPING_ODDS', 'SCRAPING-ODDS'),
        getenv('KAFKA_SCRAPING_EVENTS', 'SCRAPING-PROVIDER-EVENTS')
    ];

    $conf = new Conf();
    $conf->set('group.id', getenv('KAFKA_GROUP_ID', 'ml-db'));

    $rk = new Consumer($conf);
    $rk->addBrokers(getenv('KAFKA_BROKERS', 'kafka:9092'));

    $queue = $rk->newQueue();
    foreach ($topics as $t) {
        $topicConf = new TopicConf();
        $topicConf->set('auto.commit.interval.ms', 100);
        $topicConf->set('offset.store.method', 'broker');
        $topicConf->set('auto.offset.reset', 'latest');

        $topic = $rk->newTopic($t, $topicConf);
        logger('info', 'app', "Setting up " . $t);
        echo "Setting up " . $t . "\n";
        $topic->consumeQueueStart(0, RD_KAFKA_OFFSET_STORED, $queue);
    }

    return $queue;
}

function preProcess()
{
    global $dbPool;

    $connection = $dbPool->borrow();

    PreProcess::init($connection);
    PreProcess::loadLeagues();
    PreProcess::loadTeams();
    PreProcess::loadEvents();
    PreProcess::loadEventMarkets();

    PreProcess::loadEnabledProviders();
    PreProcess::loadEnabledSports();
    PreProcess::loadSportsOddTypes();
    preProcess::loadMaintenance();

    $dbPool->return($connection);

}

function reactor($queue)
{
    global $count;
    global $activeProcesses;

    while (true) {
        $message = $queue->consume(0);
        if ($message) {
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    logger('info', 'odds-events-reactor', 'consuming...', (array) $message);
                    if ($message->payload) {
                        $key = getPipe(getenv('ODDS_EVENTS_PROCESSES_NUMBER', 1));

                        $payload = json_decode($message->payload, true);
                        switch ($payload['command']) {
                            case 'odd':
                                // go("oddHandler", $key, $payload, $message->offset);
                                oddHandler($payload, $message->offset);
                                break;
                            case 'event':
                            default:
                                // go("eventHandler", $key, $payload, $message->offset);
                                eventHandler($payload, $message->offset);
                                break;
                        }

                        $activeProcesses++;
                        $count++;
                    }
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    // echo "Timed out\n";
                    break;
                default:
                    throw new Exception($message->errstr(), $message->err);
                    break;
            }
        } else {
            Co\System::sleep(0.001);
        }
    }
}

function oddHandler($message, $offset)
{
    global $swooleTable;
    global $dbPool;

    $start = microtime(true);

    try {

        $previousTS = $swooleTable['timestamps']['odds:' . $message["data"]["schedule"]]['ts'];
        $messageTS  = $message["request_ts"];

        if ($messageTS < $previousTS) {
            logger('info', 'odds-events-reactor', 'Validation Error: Timestamp is old', (array) $message);
            return;
        }

        $swooleTable['timestamps']['odds:' . $message["data"]["schedule"]]['ts'] = $messageTS;

        if (!is_array($message["data"]) || empty($message["data"])) {
            logger('info', 'odds-events-reactor', 'Validation Error: Invalid Payload', (array) $message);
            freeUpProcess();
            return;
        }

        $toHashMessage = $message["data"];
        unset($toHashMessage['runningtime'], $toHashMessage['id']);
        $caching = 'odds-' . md5(json_encode((array) $toHashMessage));

        if (!empty($toHashMessage['events'][0]['eventId'])) {
            $eventId = $toHashMessage['events'][0]['eventId'];

            if ($swooleTable['eventOddsHash']->exists($eventId) && $swooleTable['eventOddsHash'][$eventId]['hash'] == $caching) {
                logger('info', 'odds-events-reactor', 'Validation Error: Hash data is the same as with the current hash data', (array) $message);
                freeUpProcess();
                return;
            }
        }
        $swooleTable['eventOddsHash'][$eventId] = ['hash' => $caching];

        if (!$swooleTable['enabledSports']->exists($message["data"]["sport"])) {
            logger('info', 'odds-events-reactor', 'Validation Error: Sport is inactive', (array) $message);
            freeUpProcess();
            return;
        }

        if (!$swooleTable['enabledProviders']->exists($message["data"]["provider"])) {
            logger('info', 'odds-events-reactor', 'Validation Error: Provider is inactive', (array) $message);
            freeUpProcess();
            return;
        }

        go(function () use ($dbPool, $swooleTable, $message, $offset) {
            try {
                $connection = $dbPool->borrow();

                ProcessOdds::handle($connection, $swooleTable, $message, $offset);

                $dbPool->return($connection);
            } catch (Exception $e) {
                echo $e->getMessage();
            }

        });
    } catch (Exception $e) {
        logger('info', 'odds-events-reactor', 'Exception Error', (array) $e);
    } finally {
        freeUpProcess();
        return true;
    }
}

function eventHandler($message, $offset)
{
    global $swooleTable;
    global $dbPool;

    try {
        $previousTS = $swooleTable['timestamps']['event:' . $message["data"]["schedule"]]['ts'];
        $messageTS  = $message["request_ts"];

        if ($messageTS < $previousTS) {
            logger('info', 'odds-events-reactor', 'Validation Error: Timestamp is old', (array) $message);
            freeUpProcess();
            return;
        }

        $swooleTable['timestamps']['event:' . $message["data"]["schedule"]]['ts'] = $messageTS;

        if (!is_array($message["data"]) || empty($message["data"])) {
            logger('info', 'odds-events-reactor', 'Validation Error: Data should be valid', (array) $message);
            freeUpProcess();
            return;
        }

        if (!$swooleTable['enabledSports']->exists($message["data"]["sport"])) {
            logger('info', 'odds-events-reactor', 'Validation Error: Sport is inactive', (array) $message);
            freeUpProcess();
            return;
        }

        if (!$swooleTable['enabledProviders']->exists($message["data"]["provider"])) {
            logger('info', 'odds-events-reactor', 'Validation Error: Provider is inactive', (array) $message);
            freeUpProcess();
            return;
        }

        go(function () use ($dbPool, $swooleTable, $message, $offset) {
            try {
                $connection = $dbPool->borrow();

                ProcessEvent::handle($connection, $swooleTable, $message, $offset);

                $dbPool->return($connection);
            } catch (Exception $e) {
                echo $e->getMessage();
            }

        });
    } catch (Exception $e) {
        logger('info', 'odds-events-reactor', 'Exception Error', (array) $e);
    } finally {
        freeUpProcess();
        return true;
    }


}

function execute($payload, $key)
{
    #var_dump($payload);
    #var_dump($key);
    $myPayload = json_decode($payload);
    if ($myPayload) {
        Co\System::sleep(0.025);
        echo "c";
        freeUpProcess();
    } else {
        echo "x";
    }
}

$activeProcesses = 0;
$count = 0;
$queue           = makeConsumer();
$dbPool          = null;
makeProcess();

Co\run(function () use ($queue) {
    global $dbPool;



    // Swoole\Timer::tick(1000, "checkRate");
    $dbPool = databaseConnectionPool();

    $dbPool->init();
    defer(function () use ($dbPool) {
        logger('info', 'odds-events-reactor', "Closing connection pool");
        $dbPool->close();
    });

    preProcess();
    reactor($queue, $dbPool);
});