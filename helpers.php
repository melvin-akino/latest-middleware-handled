<?php

use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

function getPipe($maxProcessNumber = 1) {
	global $activeProcesses;

	while (true) {
		if ($activeProcesses < $maxProcessNumber) {
			return true;
		}
        Co\System::sleep(0.001);
	}
}

function makeProcess() {
	global $activeProcesses;

	$activeProcesses = 0;
}

function freeUpProcess() {
	global $activeProcesses;

	$activeProcesses--;
}

function checkRate() {
	global $count;
	global $activeProcesses;

	echo "\n" . $count . " .. [" . $activeProcesses . "]..\n";
	$count=0;
}

function databaseConnectionPool()
{
    global $config;

    $dbString = [];
    foreach ($config['database']['pgsql'] as $key => $db) {
        $dbString[] = $key . '=' . $db;
    }
    $connectionString = implode(' ', $dbString);
    $pool = new ConnectionPool(
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

function Logger($loggerName = 'app', ?string $data = null, $context = [], $extraData = [])
{
    global $log;

    $log[$loggerName]->warning($data, $context, $extraData);
}