<?php

namespace Models;

use Models\Model;

class UnmatchedData extends Model
{
    protected static $table = 'unmatched_data';

    public static function getAllUnmatchedWithSport($connection)
    {
        $sql = "SELECT ul.data_id, ul.data_type, l.provider_id, l.name, l.sport_id FROM " . static::$table . " AS ul
            JOIN leagues AS l ON l.id = ul.data_id AND data_type = 'league'

            UNION

            SELECT ut.data_id, ut.data_type, t.provider_id, t.name, t.sport_id FROM " . static::$table . " AS ut
            JOIN teams AS t ON t.id = ut.data_id AND data_type = 'team'

            UNION

            SELECT ue.data_id, ue.data_type, e.provider_id, '' AS name, e.sport_id FROM " . static::$table . " AS ue
            JOIN events AS e ON e.id = ue.data_id AND data_type = 'event'";

        return $connection->query($sql);
    }

    public static function removeToUnmatchedData($connection, array $cond)
    {
      if (!empty($cond)) {
          $where = "";
          $ctr   = 0;

          foreach ($cond AS $key => $value) {
              if ($ctr > 0) {
                  $where .= " AND ";
              } else {
                  $where .= "WHERE ";
              }

              $where .= "{$key} = '{$value}'";
              $ctr++;
          }

          $sql = "DELETE FROM " . static::$table . " {$where}";

          return $connection->query($sql);
      }

      return false;
    }

    public static function checkIfTeamHasMatchedLeague($connection, $providerId, $teamId)
    {
      $sql = "SELECT ut.data_id, ut.data_type, t.provider_id, t.sport_id, t.name 
      FROM unmatched_data AS ut
      JOIN teams AS t ON t.id = ut.data_id AND data_type = 'team'
      JOIN events as e ON t.id = e.team_home_id OR t.id = e.team_away_id
      JOIN league_groups as lg ON e.league_id = lg.league_id
      WHERE t.provider_id = $providerId AND ut.data_id = $teamId";

      $result = $connection->query($sql);

      return $connection->numRows($result);
    }
}