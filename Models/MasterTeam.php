<?php

namespace Models;

use Models\Model;

class MasterTeam extends Model
{
    protected static $table = 'master_teams';

    public static function getMatches($connection)
    {
        $sql = "SELECT mt.id as master_team_id, t.id as team_id, COALESCE(mt.name, t.name) AS name, t.provider_id, t.sport_id, string_agg(lg.master_league_id::text, ',') AS master_league_ids 
        FROM master_teams as mt
        JOIN team_groups as tg ON mt.id = tg.master_team_id
        JOIN teams as t ON t.id = tg.team_id
        JOIN events as e ON t.id = e.team_home_id OR t.id = e.team_away_id
        JOIN league_groups as lg ON e.league_id = lg.league_id
        GROUP BY mt.id, t.id, t.provider_id, t.sport_id";

        return $connection->query($sql);
    }
}