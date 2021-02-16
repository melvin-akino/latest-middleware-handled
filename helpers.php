<?php

use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;

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
    defer(function () use ($pool) {
        echo "Closing connection pool\n";
        $pool->close();
    });
    #$connection = $pool->borrow();
    #$result = $connection->query("SELECT id FROM users");
    #$stat = $connection->fetchAssoc($result);
    #$pool->return($connection);
    #echo "\n\nHERE...";
    #var_dump($stat);
    #echo "...\n";

    return $pool;
}
