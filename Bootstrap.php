<?php

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/Classes/DotEnv.php';
require_once __DIR__ . '/Classes/SwooleTable.php';
require_once __DIR__ . '/Classes/PreProcess.php';
// require_once __DIR__ . '/Classes/SwooleStats.php';

use DevCoder\DotEnv;
use RdKafka\{Conf, Producer};

(new DotEnv(__DIR__ . '/.env'))->load();

$log = null;

require_once __DIR__ . '/helpers.php';
require_once __DIR__ . '/config.php';

instantiateLogger();


$producerConf = new Conf();
$producerConf->set('metadata.broker.list', getenv('KAFKA-BROKER', 'kafka:9092'));
$kafkaProducer    = new Producer($producerConf);

$swooleTable = new SwooleTable;
foreach ($config['swoole_tables'] as $table => $details) {
    $swooleTable->createTable($table, $details);
};

$swooleTable = $swooleTable->table;

// $swooleStats = SwooleStats::getInstance();

$rootDir = __DIR__;