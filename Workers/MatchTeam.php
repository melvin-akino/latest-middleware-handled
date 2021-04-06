<?php

namespace Workers;

use Models\{
    Team,
    TeamGroup,    
    MasterTeam,
    UnmatchedData,
    Provider
};
use Carbon\Carbon;
use Co\System;
use Exception;

class MatchTeam
{
    public static function handle($dbPool, $swooleTable) 
    {
        while (true)
        {
          logger('info', 'matching', "Processing Unmatched Raw Team IDs...");

          try {
              $connection       = $dbPool->borrow();
              $primaryProvider  = Provider::getIdByAlias($connection, $swooleTable['systemConfig']['PRIMARY_PROVIDER']['value']);
              $unmatchedTeams   = $swooleTable['unmatchedTeams'];

              if ($unmatchedTeams->count()) {
                  foreach($unmatchedTeams as $key => $team) {
                      if (strpos($key, "pId:" . $primaryProvider) !== false) {
                          if(!$swooleTable['matchedTeams']->exists("pId:{$primaryProvider}:name:" . md5($team['name']))) {
                              $newMasterTeam = MasterTeam::create($connection, [
                                  'sport_id'   => $team['sport_id'],
                                  'name'       => null,
                                  'created_at' => Carbon::now()
                              ], 'id');

                              $masterTeamId = $connection->fetchArray($newMasterTeam)['id'];

                              TeamGroup::create($connection, [
                                  'master_team_id' => $masterTeamId,
                                  'team_id'        => $team['id']
                              ]);

                              $swooleTable['matchedTeams']->set("pId:{$primaryProvider}:name:" . md5($team['name']), [
                                'master_league_id' => $masterTeamId,
                                'league_id'        => $team['id'],
                                'sport_id'         => $team['sport_id'],
                                'provider_id'      => $team['provider_id'],
                              ]);
                          }

                          UnmatchedData::removeToUnmatchedData($connection, [
                            'data_id'   => $team['id'],
                            'data_type' => 'team'
                          ]);

                          $swooleTable['unmatchedTeams']->del($key);
                      }

                      if($primaryProvider != $team['provider_id']) {
              
                        if(!$swooleTable['matchedTeams']->exists("pId:" . $team['provider_id'] . ":name:" . md5($team['name']))) {

                          if($swooleTable['matchedTeams']->exists("pId:{$primaryProvider}:name:" . md5($team['name']))) {
                            $matchedTeam = $swooleTable['matchedTeams']["pId:{$primaryProvider}:name:" . md5($team['name'])];
                            $checkIfTeamHasMatchedLeague = UnmatchedData::checkIfTeamHasMatchedLeague($connection, $team['provider_id'], $team['id'], $matchedTeam['master_league_ids']);
            
                            if($checkIfTeamHasMatchedLeague == 1) {
                                TeamGroup::create($connection, [
                                    'master_team_id' => $matchedTeam['master_team_id'],
                                    'team_id'        => $team['id']
                                ]);

                                $swooleTable['matchedTeams']->set("pId:" . $team['provider_id'] . ":name:" . md5($team['name']), [
                                    'master_team_id'   => $matchedTeam['master_team_id'],
                                    'team_id'          => $team['id'],
                                    'sport_id'         => $team['sport_id'],
                                    'provider_id'      => $team['provider_id'],
                                ]);
                            }
                          } else {
                              continue;
                          }
                        }

                        UnmatchedData::removeToUnmatchedData($connection, [
                            'data_id'   => $team['id'],
                            'data_type' => 'team'
                        ]);

                        $swooleTable['unmatchedTeams']->del("pId:" . $team['provider_id'] . ":name:" . md5($team['name']));
                      }
                  }

                  logger('info', 'matching', "Done Processing Unmatched Raw Team IDs!");
              } else {
                  logger('info', 'matching', "No Unmatched Raw Team IDs processed.");
              }
          } catch (Exception $e) {
              logger('error', 'matching', "Something went wrong during Processing Unmatched Team IDs...", (array) $e);
          }
            
            $dbPool->return($connection);
            System::sleep(10);
        }
    }
}
