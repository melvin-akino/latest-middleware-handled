<?php

namespace Workers;

use Models\{
    EventMarket,
    League,
    UserWatchlist    
};
use Carbon\Carbon;
use Co\System;
use Exception;
use Ramsey\Uuid\Uuid;

class ProcessHighFrequencyMinMax
{
    public static function handle($dbPool, $providers, $primaryProviderName, $schedule) 
    {

        logger('info', 'minmax', "Processing {$schedule} event markets...");
        $providerId = array_search($primaryProviderName, $providers);
        try {
            $connection       = $dbPool->borrow();
            $userWatchlists  = UserWatchlist::getUserWatchlists($connection, $providerId, $schedule);
            $majorLeagues  = League::getMajorLeagues($connection, $providerId, $schedule);

            $events = array_unique(array_merge($userWatchlists, $majorLeagues));

            if (!empty($events)) {
                $masterEventIds = implode(",",$events);
                var_dump($masterEventIds);
                $eventMarkets = EventMarket::getMarketsByMasterEventIds($connection,$masterEventIds);
                if ($eventMarkets->count() > 0) {
                    $eventMarketArray = $connection->fetchAll($eventMarkets);
                    foreach($eventMarketArray as $market) {
                        //Push to Kafka
                        $requestId = (string) Uuid::uuid4();
                        $requestTs = getMilliseconds();
                        //Generate kafka json payload here
                        $payload = [
                            'request_uid'    => $requestId,
                            'request_ts'    => $requestTs,
                            'command'       => 'minmax',
                            'sub_command'   => 'scrape',
                            'data' => [
                                'provider'      => $providers[$market['provider_id']],
                                'sport'         => (string) $market['sport_id'],
                                'schedule'      => $market['game_schedule'],
                                'event_id'      => (string) $market['event_id'],
                                'odds'          => (string) $market['odds'],
                                'memUID'        => $market['mem_uid']
                            ]
                        ];

                        $topic = getenv('KAFKA_MINMAXHIGH', 'minmax-high_req');

                        if (!in_array(getenv('APP_ENV'), ['testing'])) {
                            kafkaPush($topic, $payload, $requestId);
                            logger('info', 'minmax', "Pushed this {$schedule} bet_identifier: ".$market['mem_uid']." - market mem_uid to kafka:".$market['mem_uid']);
                        }
                    }
                }
            } else {
                logger('info', 'minmax', "There are no {$schedule} event markets to process.");
            }
        } catch (Exception $e) {
            logger('error', 'minmax', "Something went wrong during Processing of {$schedule} event markets...", (array) $e);
        }

        $dbPool->return($connection);
    }
}
