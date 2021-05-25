<?php

namespace Workers;

use Models\{
    ProviderBet,
    ProviderBetLog,
    UserBet
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
                        logger('info', 'old-pending-bets', "Processing provider bet id: " . $key . "...");
                        $result = json_decode($redis->get(self::PLACED_BET_KEY.$key), true);
                        if ($result) {
                            kafkaPush(getenv('KAFKA_BET_PLACED', 'PLACED-BET'), $result, $result['request_uid']);
                            logger('info', 'old-pending-bets', '[PLACED-BET] Payload sent: ' . $result['request_uid']);
                        } else {
                            $status = 'FAILED';
                            $providerBetUpdate = ProviderBet::updateById($connection, $key, [
                                'status'       => $status,
                                'reason'       => 'Placement Expired',
                                'updated_at'   => Carbon::now()
                            ]);

                            $providerBetLog = ProviderBetLog::create($connection, [
                                'provider_bet_id'   => $key,
                                'status'            => $status,
                                'created_at'        => Carbon::now(),
                                'updated_at'        => Carbon::now()
                            ]);

                            $checkIfUserBetIsPending = UserBet::checkIfPending($connection, $oldPendingBet['user_bet_id']);
                            if ($connection->numRows($checkIfUserBetIsPending) > 0) {
                                $assocProviderBetsResult = ProviderBet::getByUserBetId($connection, $oldPendingBet['user_bet_id']);
                                $assocProviderBets = $connection->fetchAll($assocProviderBetsResult);
                                
                                $hasPending = false;
                                foreach($assocProviderBets as $assocProviderBet) {
                                    if ($assocProviderBet['status'] == 'PENDING') {
                                        $hasPending = true;
                                        break;
                                    } else if ($assocProviderBet['status'] != 'FAILED') {
                                        $status = $assocProviderBet['status'];
                                    }
                                }

                                if (!$hasPending) {
                                    UserBet::update($connection, [
                                        'status'        => $status,
                                        'updated_at'    => Carbon::now()
                                    ], [
                                        'id' => $oldPendingBet['user_bet_id']
                                    ]);
                                    logger('info', 'old-pending-bets', 'Updated status of user bet: ' . $oldPendingBet['user_bet_id'] . ' to ' . $status);
                                }
                            }
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