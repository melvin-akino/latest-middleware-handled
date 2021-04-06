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
                        $masterLeagueId = "";

                        if (strpos($key, "pId:{$primaryProvider}") !== false) {
                            $rawCheckIfMatched = $swooleTable['matchedLeagues']["pId:{$primaryProvider}:name:" . md5($league['name'])];

                            if (!$swooleTable['matchedLeagues']->exists("pId:{$primaryProvider}:name:" . md5($league['name']))) {
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

                                $swooleTable['matchedLeagues']->set("pId:{$primaryProvider}:name:" . md5($league['name']), [
                                    'master_league_id' => $masterLeagueId,
                                    'league_id'        => $league['id'],
                                    'sport_id'         => $league['sport_id'],
                                    'provider_id'      => $league['provider_id'],
                                ]);
                            }

                            UnmatchedData::removeToUnmatchedData($connection, [
                                'data_id'   => $league['id'],
                                'data_type' => 'league'
                            ]);

                            $swooleTable['unmatchedLeagues']->del($key);
                        }

                        if ($primaryProvider != $league['provider_id']) {
                            $rawCheckIfMatched = $swooleTable['matchedLeagues']["pId:" . $league['provider_id'] . ":name:" . md5($league['name'])];

                            if (!$swooleTable['matchedLeagues']->exists("pId:" . $league['provider_id'] . ":name:" . md5($league['name']))) {
                                $matchedLeague = $swooleTable['matchedLeagues']["pId:{$primaryProvider}:name:" . md5($league['name'])];

                                if ($swooleTable['matchedLeagues']->exists("pId:{$primaryProvider}:name:" . md5($league['name']))) {
                                    LeagueGroup::create($connection, [
                                        'master_league_id' => $matchedLeague['master_league_id'],
                                        'league_id'        => $league['id']
                                    ]);

                                    $swooleTable['matchedLeagues']->set("pId:" . $league['provider_id'] . ":name:" . md5($league['name']), [
                                        'master_league_id' => $matchedLeague['master_league_id'],
                                        'league_id'        => $league['id'],
                                        'sport_id'         => $league['sport_id'],
                                        'provider_id'      => $league['provider_id'],
                                    ]);
                                } else {
                                    continue;
                                }
                            }

                            UnmatchedData::removeToUnmatchedData($connection, [
                                'data_id'   => $league['id'],
                                'data_type' => 'league'
                            ]);

                            $swooleTable['unmatchedLeagues']->del("pId:" . $league['provider_id'] . ":name:" . md5($league['name']));
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
