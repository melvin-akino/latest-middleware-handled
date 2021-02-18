<?php

use RdKafka\{KafkaConsumer, Conf, Consumer, TopicConf};
use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;
use Workers\ProcessBalance;

require_once __DIR__ . '/../Bootstrap.php';

function makeConsumer() 
{
    // LOW LEVEL CONSUMER
    $topics = [
        getenv('KAFKA-SCRAPING-BALANCES', 'BALANCE'),
    ];

    $conf = new Conf();
	$conf->set('group.id', getenv('KAFKA-GROUP', 'ml-db'));

	$rk = new Consumer($conf);
	$rk->addBrokers(getenv('KAFKA-BROKER', 'kafka:9092'));

	$queue = $rk->newQueue();
	foreach ($topics as $t) {
		$topicConf = new TopicConf();
		$topicConf->set('auto.commit.interval.ms', 100);
		$topicConf->set('offset.store.method', 'broker');
		$topicConf->set('auto.offset.reset', 'latest');

		$topic = $rk->newTopic($t, $topicConf);
        Logger('info','balance-reactor',  "Setting up " . $t);
        echo "Setting up " . $t . "\n";
		$topic->consumeQueueStart(0, RD_KAFKA_OFFSET_STORED,$queue);
	}

    return $queue;
}

function preProcess()
{
    global $dbPool;

    $connection = $dbPool->borrow();
    $result = $connection->query("SELECT count(*) FROM users");
    $stat = $connection->fetchAssoc($result);

    PreProcess::init($connection);
    PreProcess::loadEnabledProviders();
    preProcess::loadEnabledProviderAccounts();

    $dbPool->return($connection);
    
}

function reactor($queue) {
	global $count;
    global $activeProcesses;

	while (true) {
		$message = $queue->consume(0);
		if ($message) {
			switch ($message->err) {
				case RD_KAFKA_RESP_ERR_NO_ERROR:
                    Logger('info','balance-reactor', 'consuming...', (array) $message);
					if ($message->payload) {
                        getPipe(getenv('BALANCE-PROCESSES-NUMBER', 1));

                        $payload = json_decode($message->payload, true);
                        balanceHandler($payload, $message->offset);
						
						$activeProcesses++;
						$count++;
                    }
					break;
				case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    Logger('info','balance-reactor', "No more messages; will wait for more");
					echo "No more messages; will wait for more\n";
					break;
				case RD_KAFKA_RESP_ERR__TIMED_OUT:
					// Kafka message timed out. Ignore
					break;
				default:
                    Logger('info','balance-reactor', $message->errstr(), $message->err);
					throw new Exception($message->errstr(), $message->err);
					break;
			}
		} else {
			Co\System::sleep(0.001);
		}
	}
}

function balanceHandler($message, $offset)
{
    global $swooleTable;
    global $dbPool;

    $start = microtime(true);

    try {
        $start = microtime(true);

        $previousTS = $swooleTable['timestamps']['balance']['ts'];
        $messageTS  = $message["request_ts"];
        if ($messageTS < $previousTS) {
            $statsArray = [
                "type"        => 'balance',
                "status"      => SwooleStats::getErrorType('TIMESTAMP_ERROR'),
                "time"        => microtime(true) - $start,
                "request_uid" => $message["request_uid"],
                "request_ts"  => $message["request_ts"],
                "offset"      => $offset,
            ];

            SwooleStats::addStat($statsArray);

            Logger('info','balance-reactor', 'Validation Error: Timestamp is old', (array) $message);
            return;
        }

        $swooleTable['timestamps']['balance']['ts'] = $messageTS;

        if (
            empty($message['data']['provider']) ||
            empty($message['data']['username']) ||
            empty($message['data']['available_balance']) ||
            empty($message['data']['currency'])
        ) {
            $statsArray = [
                "type"        => 'balance',
                "status"      => SwooleStats::getErrorType('ERROR'),
                "time"        => microtime(true) - $start,
                "request_uid" => $message["request_uid"],
                "request_ts"  => $message["request_ts"],
                "offset"      => $offset,
            ];
            SwooleStats::addStat($statsArray);

            Logger('info','balance-reactor', 'Validation Error: Invalid Payload', (array) $message);
            return;
        }

        if (!$swooleTable['enabledProviders']->exists($message['data']['provider'])) {
            $statsArray = [
                "type"        => 'balance',
                "status"      => SwooleStats::getErrorType('EVENTS_INACTIVE_PROVIDER'),
                "time"        => microtime(true) - $start,
                "request_uid" => $message["request_uid"],
                "request_ts"  => $message["request_ts"],
                "offset"      => $offset,
            ];
            SwooleStats::addStat($statsArray);

            Logger('info','balance-reactor', 'Validation Error: Invalid Provider', (array) $message);
            return;
        }

        go(function() use($dbPool, $swooleTable, $message, $offset) {
            try {
                $connection = $dbPool->borrow();
                $result = $connection->query("SELECT count(*) FROM users");
                $stat = $connection->fetchAssoc($result);
                // SwooleStats::addStat($statsArray);
                

                ProcessBalance::handle($connection, $swooleTable, $message, $offset);
                
                // var_dump($stat);

                $dbPool->return($connection);
            } catch (Exception $e) {
                echo $e->getMessage();
            }
            
            // ProcessOdds
        });
    } catch (Exception $e) {
        echo $e->getMessage();
        // self::$configTable["processKafka"]["value"] = 0;
        // processTaskTempDir(false);
        // self::$configTable["processKafka"]["value"] = 1;
    } finally {
        freeUpProcess();
        return true;
    }
}

$activeProcesses = 0;
$queue           = makeConsumer();
$dbPool = null;
makeProcess();

Co\run(function() use ($queue, $activeProcesses) {
    global $dbPool;

	$count = 0;

    // Swoole\Timer::tick(1000, "checkRate");
    $dbPool = databaseConnectionPool();

    $dbPool->init();
    defer(function () use ($dbPool) {
        Logger('info','balance-reactor',  "Closing connection pool");
        echo "Closing connection pool\n";
        $dbPool->close();
    });

    preProcess();
    reactor($queue);
});