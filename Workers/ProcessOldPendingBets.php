<?php

namespace Workers;

use Models\{
    Order,
    OrderLog
};
use \Swoole\Database\RedisConfig;
use Helper\RedisPool;
use Carbon\Carbon;
use Exception;

class ProcessOldPendingBets
{
    private static $swooleTable;

    const PLACED_BET_KEY = "MLBET-";

    public static function handle($connection, $swooleTable)
    {
        try {
            self::$swooleTable   = $swooleTable;
            $oldPendingBets      = $swooleTable['oldPendingBets'];

            if ($oldPendingBets->count() > 0) {
                $pool = new RedisPool((new RedisConfig())
                    ->withHost(getenv('REDIS_HOST'))
                    ->withPort(getenv('REDIS_PORT'))
                );

                $redis = $pool->get();

                foreach($oldPendingBets as $key => $oldPendingBet) {
                    try {
                        logger('info', 'old-pending-bets', "Processing order id: " . $key . "...");
                        $result = json_decode($redis->get(self::PLACED_BET_KEY.$key), true);
                        if ($result) {
                            kafkaPush(getenv('KAFKA_BET_PLACED', 'PLACED-BET'), $result, $result['request_uid']);
                            logger('info', 'old-pending-bets', '[PLACED-BET] Payload sent: ' . $result['request_uid']);
                        }
                    } catch (Exception $e1) {
                        logger('error', 'old-pending-bets', 'Error', (array) $e1);
                    }
                }
            }
            logger('info', 'old-pending-bets', "Done processing old pending bets");
        } catch (Exception $e) {
            logger('error', 'old-pending-bets', 'Error', (array) $e);
        }
    }
}