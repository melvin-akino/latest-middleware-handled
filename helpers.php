<?php

use RdKafka\{KafkaConsumer, Conf, Consumer, TopicConf};
use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Ramsey\Uuid\Uuid;

function getPipe($maxProcessNumber = 1)
{
    global $activeProcesses;

    while (true) {
        if ($activeProcesses < $maxProcessNumber) {
            return true;
        }
        Co\System::sleep(0.001);
    }
}

function makeProcess()
{
    global $activeProcesses;

    $activeProcesses = 0;
}

function freeUpProcess()
{
    global $activeProcesses;

    $activeProcesses--;
}

function checkRate()
{
    global $count;
    global $activeProcesses;

    echo "\n" . $count . " .. [" . $activeProcesses . "]..\n";
    $count = 0;
}

function databaseConnectionPool()
{
    global $config;

    $dbString = [];
    foreach ($config['database']['pgsql'] as $key => $db) {
        $dbString[] = $key . '=' . $db;
    }
    $connectionString = implode(' ', $dbString);
    $pool             = new ConnectionPool(
        $config['database']['connection_pool'],
        new CoroutinePostgreSQLConnector,
        [
            'connection_strings' => $connectionString
        ]
    );

    $pool->init();
    Co\System::sleep(0.5);
    defer(function () use ($pool) {
        echo "Closing connection pool\n";
        $pool->close();
    });

    return $pool;
}

function instantiateLogger()
{
    global $config;
    global $log;

    foreach ($config['logger'] as $key => $logConfig) {
        switch ($logConfig['level']) {
            case 'debug':
                $level = 100;
                break;
            case 'info':
                $level = 200;
                break;
            case 'error':
            default:
                $level = 400;
                break;
        }


        $log[$key] = new Logger($key);
        $log[$key]->pushHandler(new StreamHandler(__DIR__ . '/Log/' . $logConfig['name'], $level));

    }
}

function logger($type, $loggerName = 'app', ?string $data = null, $context = [], $extraData = [])
{
    global $log;

    $log[$loggerName]->{$type}($data, $context, $extraData);
}

function getMilliseconds()
{
    $mt = explode(' ', microtime());
    return bcadd($mt[1], $mt[0], 8);
}

function kafkaPush($kafkaTopic, $message, $key)
{
    global $kafkaProducer;

    try {
        $topic = $kafkaProducer->newTopic($kafkaTopic);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($message), $key);

        for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
            $result = $kafkaProducer->flush(10000);
            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }
        Logger('info', 'app', 'Sending to Kafka Topic: ' . $kafkaTopic, $message);
    } catch (Exception $e) {
        Logger('error', 'app', 'Error', (array) $e);
    }
}

function createConsumer($topics)
{
    // LOW LEVEL CONSUMER
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
        logger('info', 'app', "Setting up " . $t);
        echo "Setting up " . $t . "\n";
        $topic->consumeQueueStart(0, RD_KAFKA_OFFSET_STORED, $queue);
    }

    return $queue;
}

function getPayloadPart($command, $subCommand)
{
    $requestId = (string) Uuid::uuid4();
    $requestTs = getMilliseconds();

    return [
        'request_uid' => $requestId,
        'request_ts'  => $requestTs,
        'command'     => $command,
        'sub_command' => $subCommand
    ];
}