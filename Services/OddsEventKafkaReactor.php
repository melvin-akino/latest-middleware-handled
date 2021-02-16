<?php

use RdKafka\KafkaConsumer;
use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Runtime;

require_once __DIR__ . '/../Bootstrap.php';

function makeConsumer() {
    global $kafkaConf;

    $kafkaConsumer = new KafkaConsumer($kafkaConf);
    $kafkaConsumer->subscribe([
        getenv('KAFKA-SCRAPING-ODDS', 'SCRAPING-ODDS'), 
        getenv('KAFKA-SCRAPING-EVENTS', 'SCRAPING-PROVIDER-EVENTS')
    ]);

    echo "Setting Up Odds & Events Scraping Consumer";

	return $kafkaConsumer;
}

// function databaseConnectionPool()
// {
//     $pool = new ConnectionPool(
//         [
//             'minActive'         => 10,
//             'maxActive'         => 30,
//             'maxWaitTime'       => 5,
//             'maxIdleTime'       => 20,
//             'idleCheckInterval' => 10,
//         ],
//         new CoroutinePostgreSQLConnector,
//         [
//             'connection_strings' => "host=" . getenv('DB_HOST', 'db') . " port=" . getenv('DB_PORT', 5432) . " dbname=" . getenv('DB_DATABASE', 'multilinev2') . " user=" . getenv('DB_USERNAME', 'postgres') . " password=" . getenv('DB_PASSWORD', 'password'),
//         ]
//     );
    

//     $pool->init();

//     $connection = $pool->borrow();
//     $status = $connection->query('SELECT id FROM users');
//     $stat = $connection->fetchAssoc($status);
    

//     echo "Return the connection to pool as soon as possible\n";
//     $pool->return($connection);

//     var_dump($stat);
// }

function reactor($queue) {
	global $count;
    global $activeProcesses;
	while (true) {
		$message = $queue->consume(0);
		if ($message) {
			switch ($message->err) {
				case RD_KAFKA_RESP_ERR_NO_ERROR:
					if ($message->payload) {
                        $key = getPipe(getenv('ODDS-EVENTS-PROCESSES-NUMBER', 1));

                        $payload = json_decode($message->payload, true);
                        switch($payload['command']) {
                            case 'odd':
                                // go("oddHandler", $key, $payload, $message->offset);
                                oddHandler($key, $payload, $message->offset);
                                break;
                            case 'event':
                            default:
                                // go("eventHandler", $key, $payload, $message->offset);
                                eventHandler($key, $payload, $message->offset);
                                break;
                        }
						
						$activeProcesses++;
						$count++;
                    }
                    $queue->commitAsync($message);
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

function oddHandler($key, $message, $offset)
{
    global $swooleTable;
    global $dbPool;

    $start = microtime(true);

    try {

        // $previousTS = $swooleTable['timestamps']['odds']['ts'];
        // $messageTS  = $message["request_ts"];

        // if ($messageTS < $previousTS) {
        //     $statsArray = [
        //         "type"        => "odds",
        //         // "status"      => $swooleStats::getErrorType('TIMESTAMP_ERROR'),
        //         "time"        => microtime(true) - $start,
        //         "request_uid" => $message["request_uid"],
        //         "request_ts"  => $message["request_ts"],
        //         "offset"      => $offset,
        //     ];

        //     // SwooleStats::addStat($statsArray);
        //     freeUpProcess();
        //     return;
        // }

        // if (!is_array($message["data"]) || empty($message["data"])) {
        //     $statsArray = [
        //         "type"        => "odds",
        //         // "status"      => SwooleStats::getErrorType('ERROR'),
        //         "time"        => microtime(true) - $start,
        //         "request_uid" => $message["request_uid"],
        //         "request_ts"  => $message["request_ts"],
        //         "offset"      => $offset,
        //     ];
        //     // SwooleStats::addStat($statsArray);
        //     freeUpProcess();
        //     return;
        // }

        // $toHashMessage = $message["data"];
        // unset($toHashMessage['runningtime'], $toHashMessage['id']);
        // $caching = 'odds-' . md5(json_encode((array) $toHashMessage));

        // if (!empty($toHashMessage['events'][0]['eventId'])) {
        //     $eventId = $toHashMessage['events'][0]['eventId'];

        //     if ($swooleTable['eventOddsHash']->exists($eventId) && $swooleTable['eventOddsHash'][$eventId]['hash'] == $caching) {
        //         $statsArray = [
        //             "type"        => "odds",
        //             // "status"      => SwooleStats::getErrorType('HASH_ERROR'),
        //             "time"        => microtime(true) - $start,
        //             "request_uid" => $message["request_uid"],
        //             "request_ts"  => $message["request_ts"],
        //             "offset"      => $offset,
        //         ];
        //         // SwooleStats::addStat($statsArray);
        //         freeUpProcess();
        //         return;
        //     }
        // }
        // $swooleTable['eventOddsHash'][$eventId] = ['hash' => $caching];

        // if (!$swooleTable['enabledSports']->exists($message["data"]["sport"])) {
        //     $statsArray = [
        //         "type"        => "odds",
        //         // "status"      => SwooleStats::getErrorType('EVENTS_INACTIVE_SPORT'),
        //         "time"        => microtime(true) - $start,
        //         "request_uid" => $message["request_uid"],
        //         "request_ts"  => $message["request_ts"],
        //         "offset"      => $offset,
        //     ];
        //     // SwooleStats::addStat($statsArray);
        //     freeUpProcess();
        //     return;
        // }

        // if (!$$swooleTable['enabledProvidersTable']->exists($message["data"]["provider"])) {
        //     $statsArray = [
        //         "type"        => "odds",
        //         "status"      => SwooleStats::getErrorType('EVENTS_INACTIVE_PROVIDER'),
        //         "time"        => microtime(true) - $start,
        //         "request_uid" => $message["request_uid"],
        //         "request_ts"  => $message["request_ts"],
        //         "offset"      => $offset,
        //     ];
        //     // SwooleStats::addStat($statsArray);
        //     freeUpProcess();
        //     return;
        // }

        echo 1;
        go(function() use($dbPool) {
            echo 2;
            try {

                echo 3;
                $dbPool->init();
                echo 4;
                $connection = $dbPool->borrow();    
                echo 5;
                $result = $connection->query("SELECT id FROM users");
                echo 6;
                $stat = $connection->fetchAssoc($result);
                $pool->return($connection);
                echo 7;
                var_dump($stat);
            } catch (Exception $e) {
                echo 9;
                echo $e->getMessage();
            }

            freeUpProcess();
            // ProcessOdds
        });
    } catch (Exception $e) {
        echo $e->getMessage();
        // self::$configTable["processKafka"]["value"] = 0;
        // processTaskTempDir(false);
        // self::$configTable["processKafka"]["value"] = 1;
    } finally {
        
        return true;
    }
}

function eventHandler($payload, $key)
{
    freeUpProcess();
}

function execute($payload,$key) {
	#var_dump($payload);
	#var_dump($key);
	$myPayload=json_decode($payload);
	if($myPayload) {
		Co\System::sleep(0.025);
		echo "c";
		freeUpProcess();
	}
	else {
		echo "x";
	}
}

$activeProcesses = 0;
$queue           = makeConsumer();
$dbPool = databaseConnectionPool();
makeProcess();

Runtime::enableCoroutine();
Co\run(function() use ($queue, $activeProcesses) {
    global $dbPool;

	$count = 0;

    Swoole\Timer::tick(1000, "checkRate");
    
    $dbPool->init();
    defer(function () use ($dbPool) {
        echo "Closing connection pool\n";
        $dbPool->close();
    });
    $connection = $dbPool->borrow();
    $result = $connection->query("SELECT id FROM users");
    $stat = $connection->fetchAssoc($result);
    $dbPool->return($connection);
    var_dump($stat);

    reactor($queue);
});