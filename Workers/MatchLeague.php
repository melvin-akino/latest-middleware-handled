<?php

namespace Workers;

use Models\{League, LeagueGroup, MasterLeague, Provider, UnmatchedData};
use Carbon\Carbon;
use Co\System;
use Exception;

class MatchLeague
{
    public static function handle($dbPool, $swooleTable) 
    {
        while (true) {
            logger('info', 'matching', "Processing Unmatched Raw League IDs...");

            try {
                $connection       = $dbPool->borrow();
                $primaryProvider  = Provider::getIdByAlias($connection, $swooleTable['systemConfig']['PRIMARY_PROVIDER']['value']);
                $unmatchedLeagues = $swooleTable['unmatchedLeagues'];

                if ($unmatchedLeagues->count()) {
                    foreach($unmatchedLeagues as $key => $league) {
                        if (strpos($key, "providerId:" . $primaryProvider) !== false) {
                            $newMasterLeague = MasterLeague::create($connection, [
                                'sport_id'   => $league['sport_id'],
                                'name'       => null,
                                'created_at' => Carbon::now()
                            ], 'id');
                
                            $masterLeagueId = $connection->fetchArray($newMasterLeague)['id'];
                
                            LeagueGroup::create($connection, [
                                'master_league_id' => $masterLeagueId,
                                'league_id'        => $league['id']
                            ]);

                            UnmatchedData::removeToUnmatchedData($connection, [
                                'data_id'   => $league['id'],
                                'data_type' => 'league'
                            ]);

                            $swooleTable['unmatchedLeagues']->del($key);
                        }
                    }

                    logger('info', 'matching', "Done Processing Unmatched Raw League IDs!");
                } else {
                    logger('info', 'matching', "No Unmatched Raw League IDs processed.");
                }
            } catch (Exception $e) {
                logger('error', 'matching', "Something went wrong during Processing Unmatched Raw League IDs...", (array) $e);
            }

            $dbPool->return($connection);
            System::sleep(10);
        }
    }
}
