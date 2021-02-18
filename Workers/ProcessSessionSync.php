<?php

namespace Workers;

use Models\ProviderAccount;
use Ramsey\Uuid\Uuid;

class ProcessSessionSync 
{
    protected $message;
    protected $offset;
    protected $providerTypes;

    private $statsArray;

    public $channel = "session-sync";

    public static function handle($connection, $swooleTable, $message, $offset)
    {
        $providerTypes = [
            'BET_NORMAL'      => 'bet',
            'BET_VIP'         => 'bet',
            'SCRAPER'         => 'odds',
            'SCRAPER_MIN_MAX' => 'minmax',
        ];
        try {
            $providersTable   = $swooleTable['enabledProviders'];
            $providerId       = $providersTable[strtolower($message['data']['provider'])]['value'];
            $result = ProviderAccount::getByProviderAndTypes($connection, $providerId, $providerTypes);


            while ($row = $connection->fetchAssoc($result)) {
                if ($row['is_enabled']) {
                    $command = "add";
                    $data    = [
                        "provider" => strtolower($message['data']['provider']),
                        "username" => $row['username'],
                        "password" => $row['password'],
                        "category" => $providerTypes[$row->type],
                    ];
                } else {
                    $command = "stop";
                    $data    = [
                        "provider" => strtolower($message['data']['provider']),
                        "username" => $row['username'],
                    ];
                }

                $payload = [
                    "request_uid" => (string) Uuid::uuid4(),
                    "request_ts"  => getMilliseconds(),
                    "command"     => "session",
                    "sub_command" => $command,
                    "data"        => $data,
                ];

                kafkaPush(strtolower($message['data']['provider']) . getenv('KAFKA-SESSION-REQUEST-POSTFIX', '_session_req'), $payload, $payload['request_uid']);
            }

            Logger('error', 'session-sync', 'Session Sync Processed', $message);
        } catch (Exception $e) {
            Logger('error', 'session-sync', 'Error', (array) $e);
        }
    }
}
