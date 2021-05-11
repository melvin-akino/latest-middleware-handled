<?php

namespace Workers;

use Models\{
    EventMarket,
    UserWatchlist, 
};
use Carbon\Carbon;
use Co\System;
use Exception;
use Ramsey\Uuid\Uuid;

class ProcessUserWatchlist
{
    public static function handle($dbPool, $providers) 
    {
        while (true)
        {
            logger('info', 'minmax', "Processing User Watchlist markets...");

            try {
                $connection       = $dbPool->borrow();
                $userWatchlists  = UserWatchlist::getUserWatchlists($connection);

                if ($userWatchlists) {
                    $masterEventIds = [];
                    $masterEventIdArray = $connection->fetchAll($userWatchlists);
                    foreach($masterEventIdArray as $event) {
                        $masterEventIds[] = $event['master_event_id'];
                    }
                    $masterEventIds = implode(",",$masterEventIds);

                    $eventMarkets = EventMarket::getMarketsByMasterEventIds($connection,$masterEventIds);
                    if ($eventMarkets) {
                        $eventMarketArray = $connection->fetchAll($eventMarkets);
                        foreach($eventMarketArray as $market) {
                            //Push to Kafka
                            $requestId = (string) Uuid::uuid4();
                            $requestTs = getMilliseconds();
                            //Generate kafka json payload here
                            $payload = [
                                'request_id'    => $requestId,
                                'request_ts'    => $requestTs,
                                'command'       => 'minmax',
                                'sub_command'   => 'scrape',
                                'data' => [
                                    'provider'      => $providers[$market['provider_id']],
                                    'sport'         => (string) $market['sport_id'],
                                    'schedule'      => $market['game_schedule'],
                                    'event_id'      => $market['event_id'],
                                    'odds'          => $market['odds'],
                                    'memUID'        => $market['mem_uid']
                                ]

                            ];

                            $topic = getenv('KAFKA_MINMAXLOW', 'minmax-low_req');

                            if (!in_array(getenv('APP_ENV'), ['testing'])) {
                                kafkaPush($topic, $payload, $requestId);
                                logger('info', 'minmax', "Pushed this user watchlist market mem_uid to kafka:".$market['mem_uid']);
                            }
                        }
                    }
                } else {
                    logger('info', 'minmax', "There are no user watchlist markets to process.");
                }
            } catch (Exception $e) {
                logger('error', 'minmax', "Something went wrong during Processing of user watchlists...", (array) $e);
            }

            $dbPool->return($connection);
            System::sleep(30);
        }
    }
}
