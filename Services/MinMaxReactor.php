<?php
use Smf\ConnectionPool\ConnectionPool;
use Smf\ConnectionPool\Connectors\CoroutinePostgreSQLConnector;
use Swoole\Coroutine\PostgreSQL;
use Workers\ProcessMinMax;

require_once __DIR__ . '/../Bootstrap.php';


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

    $schedules = array(
        "inplay" => getenv('MINMAX_FREQUENCY_INPLAY'), 
        "today" => getenv('MINMAX_FREQUENCY_TODAY'),
        "early" => getenv('MINMAX_FREQUENCY_EARLY')
    );
    foreach($schedules as $schedule=>$interval) {
        go(function () use ($dbPool) {
            while (true) {
                try {
                    $connection = $dbPool->borrow();
                    ProcessMinMax::handle($connection, $schedule);
                    $dbPool->return($connection);
                } catch (Exception $e) {
                    echo $e->getMessage();
                }
                Co::sleep($interval);
            }
        });
    }
});