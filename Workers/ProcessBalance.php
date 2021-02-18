<?php

namespace Workers;

use Models\ProviderAccount;

class ProcessBalance
{
    protected $message;
    protected $offset;

    private $statsArray;
    public $channel = "balance";

    /**
     * Create a new Task instance.
     *
     * @return void
     */
    // public function __construct($message, $offset)
    // {
    //     $this->message    = $message;
    //     $this->offset     = $offset;
    //     $this->statsArray = [
    //         "type"        => $this->channel,
    //         "status"      => SwooleStats::NO_ERROR,
    //         "time"        => 0,
    //         "request_uid" => $this->message["request_uid"],
    //         "request_ts"  => $this->message["request_ts"],
    //         "offset"      => $offset,
    //     ];
    // }

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
            // var_dump($providerAccountsTable->count());
            foreach ($providerAccountsTable as $k => $pa) {
                if (
                    $pa['username'] == $username &&
                    $pa['provider_id'] == $providerId &&
                    $pa['credits'] != $availableBalance
                ) {
                    $result = ProviderAccount::updateBalance($connection, $username, $providerId, $availableBalance);
                    if ($result) {
                        $providerAccountsTable[$k]['credits'] = $availableBalance;
                        Logger('info','balance',  "Balance Processed", $message['data']);
                    } else {
                        throw new Exception('Something went wrong');
                    }
                    // $statsArray = [
                    //     "type"        => $this->channel,
                    //     "status"      => SwooleStats::NO_ERROR,
                    //     "time"        => microtime(true) - $startTime,
                    //     "request_uid" => $message["request_uid"],
                    //     "request_ts"  => $message["request_ts"],
                    //     "offset"      => $offset,
                    // ];
                    // SwooleStats::addStat($statsArray);
                    break;
                }
            }

            // Set the end time of the process, to keep running stats.
            $statsArray["time"] = microtime(true) - $startTime;
            // Report the stats to the Swoole Table
            // SwooleStats::addStat($this->statsArray);
        } catch (Exception $e) {
            Logger('error','balance',  "Error", (array) $e);
        }
    }
}
