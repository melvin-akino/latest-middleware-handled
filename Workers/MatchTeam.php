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
                      if (strpos($key, "providerId:" . $primaryProvider) !== false) {
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

                          UnmatchedData::removeToUnmatchedData($connection, [
                            'data_id'   => $team['id'],
                            'data_type' => 'team'
                          ]);
                          $swooleTable['unmatchedTeams']->del($key);
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
