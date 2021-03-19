<?php

namespace Workers;

use Models\{
    SystemConfiguration,
    Order
};
use Carbon\Carbon;
use Co\System;

class RequestSettlement
{
    public static function handle($dbPool, $swooleTable)
    {
        try {
            $settlementTime            = 0;
            $systemConfigurationsTimer = null;

            $connection              = $dbPool->borrow();
            $refreshDBIntervalResult = SystemConfiguration::getSettlementRequestInterval($connection);
            $refreshDBInterval       = $connection->fetchArray($refreshDBIntervalResult);
            $dbPool->return($connection);

            while (true) {
                $connection = $dbPool->borrow();
                $response   = self::logicHandler($connection, $swooleTable, $settlementTime, $refreshDBInterval['value'], $systemConfigurationsTimer);
                $dbPool->return($connection);

                if ($response) {
                    $settlementTime = $response;
                }

                System::sleep(1);
            }
        } catch (Exception $e) {
            logger('error', 'app', 'Something went wrong', $e);
        }
    }

    public static function logicHandler($connection, $swooleTable, $settlementTime, $refreshDBInterval, &$systemConfigurationsTimer)
    {
        if (empty($refreshDBInterval)) {
            $settlementTime++;
            logger('error', 'app', 'No refresh DB interval');
            return $settlementTime;
        }

        if ($settlementTime % $refreshDBInterval == 0) {
            $settlementTimerResult = SystemConfiguration::getSettlementRequestTimer($connection);
            $settlementTimer       = $connection->fetchArray($settlementTimerResult);
            if ($settlementTimer) {
                $systemConfigurationsTimer = $settlementTimer['value'];
            }
        }

        if ($systemConfigurationsTimer) {
            foreach ($swooleTable['enabledSports'] as $sportId => $sRow) {
                if ($settlementTime % (int) $systemConfigurationsTimer == 0) {
                    foreach ($swooleTable['providerAccounts'] as $paId => $pRow) {
                        $providerAlias = strtolower($pRow['alias']);
                        $username      = $pRow['username'];
                        $command       = 'settlement';
                        $subCommand    = 'scrape';

                        if ($swooleTable['maintenance']->exists($providerAlias) && empty($swooleTable['maintenance'][$providerAlias]['under_maintenance'])) {
                            $providerUnsettledDatesResult = Order::getUnsettledDates($connection, $paId);

                            // if (!empty($providerUnsettledDates)) {
                            while ($providerUnsettledDate = $connection->fetchAssoc($providerUnsettledDatesResult)) {
                                // foreach ($providerUnsettledDates as $providerUnsettledDate) {
                                $payload         = getPayloadPart($command, $subCommand);
                                $payload['data'] = [
                                    'sport'           => $sportId,
                                    'provider'        => $providerAlias,
                                    'username'        => $username,
                                    'settlement_date' => Carbon::createFromFormat('Y-m-d', $providerUnsettledDate['unsettled_date'])->subDays(1)->format('Y-m-d'),
                                ];

                                kafkaPush($providerAlias . getenv('KAFKA_SCRAPE_SETTLEMENT_POSTFIX', '_settlement_req'), $payload, $payload['request_uid']);

                                // add sleep to prevent detecting as bot
                                $sleepTime      = rand(1, 3);
                                $settlementTime += $sleepTime;
                                System::sleep($sleepTime);

                                if (Carbon::now()->format('Y-m-d') != Carbon::createFromFormat('Y-m-d', $providerUnsettledDate['unsettled_date'])->format('Y-m-d')) {
                                    $payload         = getPayloadPart($command, $subCommand);
                                    $payload['data'] = [
                                        'sport'           => $sportId,
                                        'provider'        => $providerAlias,
                                        'username'        => $username,
                                        'settlement_date' => Carbon::createFromFormat('Y-m-d', $providerUnsettledDate['unsettled_date'])->format('Y-m-d'),
                                    ];

                                    kafkaPush($providerAlias . getenv('KAFKA_SCRAPE_SETTLEMENT_POSTFIX', '_settlement_req'), $payload, $payload['request_uid']);

                                    logger('info', 'app', $providerAlias . getenv('KAFKA_SCRAPE_SETTLEMENT_POSTFIX', '_settlement_req') . " Payload Sent", $payload);

                                    // add sleep to prevent detecting as bot
                                    $sleepTime      = rand(1, 3);
                                    $settlementTime += $sleepTime;
                                    System::sleep($sleepTime);
                                }
                            }

                            $payload         = getPayloadPart($command, $subCommand);
                            $payload['data'] = [
                                'sport'           => $sportId,
                                'provider'        => $providerAlias,
                                'username'        => $username,
                                'settlement_date' => Carbon::now()->subHours(5)->format('Y-m-d'),
                            ];

                            kafkaPush($providerAlias . getenv('KAFKA_SCRAPE_SETTLEMENT_POSTFIX', '_settlement_req'), $payload, $payload['request_uid']);

                            logger('info', 'app', $providerAlias . getenv('KAFKA_SCRAPE_SETTLEMENT_POSTFIX', '_settlement_req') . " Payload Sent", $payload);

                            // add sleep to prevent detecting as bot
                            $sleepTime      = rand(60, 300);
                            $settlementTime += $sleepTime;
                            System::sleep($sleepTime);

                            // }
                        } else {
                            $settlementTime++;
                        }
                    }
                }
            }
            $settlementTime++;
            return $settlementTime;
        }
    }
}
