<?php

use RdKafka\KafkaConsumer;
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
        logger('info','app',  "Setting up " . $t);
        echo "Setting up " . $t . "\n";
		$topic->consumeQueueStart(0, RD_KAFKA_OFFSET_STORED,$queue);
	}

    return $queue;
}

function preProcess()
{
    global $dbPool;

    $connection = $dbPool->borrow();

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
                    logger('info','balance-reactor', 'consuming...', (array) $message);
					if ($message->payload) {
                        getPipe(getenv('BALANCE-PROCESSES-NUMBER', 1));

                        $payload = json_decode($message->payload, true);
                        balanceHandler($payload, $message->offset);
						
						$activeProcesses++;
						$count++;
                    }
					break;
				case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    logger('info','balance-reactor', "No more messages; will wait for more");
					echo "No more messages; will wait for more\n";
					break;
				case RD_KAFKA_RESP_ERR__TIMED_OUT:
					// Kafka message timed out. Ignore
					break;
				default:
                    logger('info','balance-reactor', $message->errstr(), $message->err);
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

    try {

        $previousTS = $swooleTable['timestamps']['balance']['ts'];
        $messageTS  = $message["request_ts"];
        if ($messageTS < $previousTS) {
            logger('info','balance-reactor', 'Validation Error: Timestamp is old', (array) $message);
            return;
        }

        $swooleTable['timestamps']['balance']['ts'] = $messageTS;

        if (
            empty($message['data']['provider']) ||
            empty($message['data']['username']) ||
            empty($message['data']['available_balance']) ||
            empty($message['data']['currency'])
        ) {
            logger('info','balance-reactor', 'Validation Error: Invalid Payload', (array) $message);
            return;
        }

        if (!$swooleTable['enabledProviders']->exists($message['data']['provider'])) {
            logger('info','balance-reactor', 'Validation Error: Invalid Provider', (array) $message);
            return;
        }

        go(function() use($dbPool, $swooleTable, $message, $offset) {
            try {
                $connection = $dbPool->borrow();

                ProcessBalance::handle($connection, $swooleTable, $message, $offset);

                $dbPool->return($connection);
            } catch (Exception $e) {
                echo $e->getMessage();
            }
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
$dbPool          = null;
makeProcess();

Co\run(function() use ($queue, $activeProcesses) {
    global $dbPool;

	$count = 0;

    // Swoole\Timer::tick(1000, "checkRate");
    $dbPool = databaseConnectionPool();

    $dbPool->init();
    defer(function () use ($dbPool) {
        logger('info', 'balance-reactor',  "Closing connection pool");
        echo "Closing connection pool\n";
        $dbPool->close();
    });

    preProcess();
    reactor($queue);
});