<?php

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/Classes/DotEnv.php';
require_once __DIR__ . '/Classes/SwooleTable.php';
// require_once __DIR__ . '/Classes/SwooleStats.php';

use DevCoder\DotEnv;
use RdKafka\Conf;

(new DotEnv(__DIR__ . '/.env'))->load();

$log = null;

require_once __DIR__ . '/helpers.php';
require_once __DIR__ . '/config.php';

instantiateLogger();


$kafkaConf = new Conf();
$kafkaConf->set('metadata.broker.list', 'kafka:9092');
$kafkaConf->set('group.id', getenv('KAFKA-GROUP', 'ml-db'));
$kafkaConf->set('auto.offset.reset', 'latest');
$kafkaConf->set('enable.auto.commit', 'false');

$swooleTable = new SwooleTable;
foreach ($config['swoole_tables'] as $table => $details) {
    $swooleTable->createTable($table, $details);
};

$swooleTable = $swooleTable->table;

// $swooleStats = SwooleStats::getInstance();

$rootDir = __DIR__;