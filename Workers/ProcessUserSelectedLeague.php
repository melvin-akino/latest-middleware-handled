<?php

namespace Workers;

use Models\{
    EventMarket,
    MasterEvent,
    UserSelectedLeague, 
};
use Carbon\Carbon;
use Co\System;
use Exception;
use Ramsey\Uuid\Uuid;

class ProcessUserSelectedLeague
{
    public static function handle($dbPool, $providers) 
    {
        while (true)
        {
            logger('info', 'minmax', "Processing User Selected Leagues markets...");

            try {
                $connection       = $dbPool->borrow();
                $userLeagues  = UserSelectedLeague::getUserSelectedLeagues($connection);

                if ($userLeagues) {
                    $masterLeagueIds = [];
                    $masterLeagueIdArray = $connection->fetchAll($userLeagues);
                    foreach($masterLeagueIdArray as $league) {
                        $masterLeagueIds[] = $league['master_league_id'];
                    }
                    $masterLeagueIdList = implode(",",$masterLeagueIds);
                    $masterEvents = MasterEvent::getMasterEventIdByMasterLeagueId($connection, $masterLeagueIdList);

                    if ($masterEvents) {
                        $masterEventId = [];
                        $masterEventIdArray = $connection->fetchAll($masterEvents);
                        foreach($masterEventIdArray as $event) {
                            $masterEventId[] = $event['id'];
                        }
                        $masterEventIds = implode(",",$masterEventId);
                        //var_dump($masterEventIds);
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
                                        'event_id'      => (string) $market['event_id'],
                                        'odds'          => (string) $market['odds'],
                                        'memUID'        => $market['mem_uid']
                                    ]

                                ];
                                $topic = getenv('KAFKA_MINMAXLOW', 'minmax-low_req');

                                if (!in_array(getenv('APP_ENV'), ['testing'])) {
                                    kafkaPush($topic, $payload, $requestId);
                                    logger('info', 'minmax', "Pushed this user selected league market mem_uid to kafka:".$market['mem_uid']);
                                }
                            }
                        }
                    }
                } else {
                    logger('info', 'minmax', "There are no user selected leagues markets to process.");
                }
            } catch (Exception $e) {
                logger('error', 'minmax', "Something went wrong during Processing of user watchlists...", (array) $e);
            }

            $dbPool->return($connection);
            System::sleep(10);
        }
    }
}
