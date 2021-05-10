<?php

namespace Workers;

use Models\{
    EventMarket,
    UserWatchlist, 
};
use Carbon\Carbon;
use Co\System;
use Exception;

class ProcessUserWatchlist
{
    public static function handle($dbPool) 
    {
        while (true)
        {
            logger('info', 'minmax', "Processing User Watchlist...");

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
                            var_dump($market['bet_identifier']);
                            //Redis LPUSH HERE
                        }
                    }
                } else {
                    logger('info', 'minmax', "There are no user watchlist to process.");
                }
            } catch (Exception $e) {
                logger('error', 'minmax', "Something went wrong during Processing of user watchlists...", (array) $e);
            }

            $dbPool->return($connection);
            System::sleep(10);
        }
    }
}
