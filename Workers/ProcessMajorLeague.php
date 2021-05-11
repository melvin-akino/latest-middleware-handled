<?php

namespace Workers;
use Classes\RedisService;
use Models\{
    EventMarket,
    MasterEvent,
    League, 
};
use Carbon\Carbon;
use Co\System;
use Exception;


class ProcessMajorLeague
{
    public static function handle($dbPool) 
    {
        while (true)
        {
            //logger('info', 'minmax', "Processing User Watchlist...");

            try {
                $connection       = $dbPool->borrow();
                $majorLeagues  = League::getMajorLeagues($connection);

                if ($majorLeagues) {
                    $masterLeagueIds = [];
                    $masterLeagueIdArray = $connection->fetchAll($majorLeagues);
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
                                var_dump($market['bet_identifier']);
                                
                                //Redis LPUSH HERE
                                $client = new RedisService();
                                if (!$client->exists('hg-minmax-high:queue', $market['bet_identifier']))
                                {
                                    $client->lpush('hg-minmax-high:queue', $market['bet_identifier']);
                                }
                            }
                        }
                    }
                } else {
                    //logger('info', 'minmax', "There are no user watchlist to process.");
                }
            } catch (Exception $e) {
                //logger('error', 'minmax', "Something went wrong during Processing of user watchlists...", (array) $e);
            }

            $dbPool->return($connection);
            System::sleep(10);
        }
    }
}
