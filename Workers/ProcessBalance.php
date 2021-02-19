<?php

namespace Workers;

use Models\ProviderAccount;

class ProcessBalance
{
    public static function handle($connection, $swooleTable, $message, $offset)
    {
        try {
            // Set the starting time of this function, to keep running stats
            $startTime = microtime(true);

            $providerAccountsTable = $swooleTable['providerAccounts'];
            $providersTable        = $swooleTable['enabledProviders'];

            $username         = $message['data']['username'];
            $provider         = $message['data']['provider'];
            $availableBalance = $message['data']['available_balance'];

            $providerId = $providersTable[$provider]['value'];
            foreach ($providerAccountsTable as $k => $pa) {
                if (
                    $pa['username']     == $username &&
                    $pa['provider_id']  == $providerId &&
                    $pa['credits']      != $availableBalance
                ) {
                    $result = ProviderAccount::updateBalance($connection, $username, $providerId, $availableBalance);
                    if ($result) {
                        $providerAccountsTable[$k]['credits'] = $availableBalance;
                        logger('info','balance',  "Balance Processed", $message['data']);
                    } else {
                        throw new Exception('Something went wrong');
                    }
                    break;
                }
            }
        } catch (Exception $e) {
            logger('error','balance',  "Error", (array) $e);
        }
    }
}
