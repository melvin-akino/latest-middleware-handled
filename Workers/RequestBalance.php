<?php

namespace Workers;

use Models\SystemConfiguration;
use Co\System;

class RequestBalance
{
    public static function handle($dbPool, $swooleTable)
    {
        try {
            $balanceTime                = 0;
            $systemConfigurationsTimers = [];

            $connection              = $dbPool->borrow();
            $refreshDBIntervalResult = SystemConfiguration::getBalanceRequestInterval($connection);
            $refreshDBInterval       = $connection->fetchArray($refreshDBIntervalResult);
            $dbPool->return($connection);

            while (true) {
                $connection = $dbPool->borrow();
                $response   = self::logicHandler($connection, $swooleTable, $balanceTime, $refreshDBInterval['value'], $systemConfigurationsTimers);
                $dbPool->return($connection);

                if ($response) {
                    $balanceTime = $response;
                }

                System::sleep(1);
            }
        } catch (Exception $e) {
            logger('error', 'app', 'Something went wrong', $e);
        }

    }

    public static function logicHandler($connection, $swooleTable, $balanceTime, $refreshDBInterval, &$systemConfigurationsTimers)
    {
        if (empty($refreshDBInterval)) {
            $balanceTime++;
            logger('error', 'app', 'No refresh DB interval');
            return $balanceTime;
        }

        if ($balanceTime % $refreshDBInterval == 0) {
            $betNormalResult = SystemConfiguration::getBetConfig($connection, 'BET_NORMAL');
            $betNormal       = $connection->fetchArray($betNormalResult);
            if ($betNormal) {
                $systemConfigurationsTimers['BET_NORMAL'] = $betNormal['value'];
            }
            $betVIPResult = SystemConfiguration::getBetConfig($connection, 'BET_VIP');
            $betVIP       = $connection->fetchArray($betVIPResult);
            if ($betVIP) {
                $systemConfigurationsTimers['BET_VIP'] = $betVIP['value'];
            }

            logger('info', 'app', 'refresh request interval');
        }

        if (!empty($systemConfigurationsTimers)) {
            foreach ($systemConfigurationsTimers as $key => $systemConfigurationsTimer) {
                if ($balanceTime % (int) $systemConfigurationsTimer == 0) {
                    self::sendKafkaPayload($swooleTable, getenv('KAFKA_SCRAPE_BALANCE_POSTFIX', '_balance_req'), 'balance', 'scrape');
                    break;
                }
            }
        }

        $balanceTime++;
        return $balanceTime;
    }

    public static function sendKafkaPayload($swooleTable, $topic, $command, $subcommand, $sportId = null)
    {
        $providerAccountsTable = $swooleTable['providerAccounts'];
        $maintenanceTable      = $swooleTable['maintenance'];

        foreach ($providerAccountsTable as $key => $providerAccount) {
            $username        = $providerAccount['username'];
            $provider        = strtolower($providerAccount['alias']);
            $payload         = getPayloadPart($command, $subcommand);
            $payload['data'] = [
                'provider' => $provider,
                'username' => $username
            ];

            if ($sportId) {
                $payload['data']['sport'] = $sportId;
            }

            if ($maintenanceTable->exists($provider) && empty($maintenanceTable[$provider]['under_maintenance'])) {
                kafkaPush($provider . $topic, $payload, $payload['request_uid']);

                logger('info', 'app', $provider . $topic . " Payload Sent", $payload);

                System::sleep(1);
            }
        }
    }
}
