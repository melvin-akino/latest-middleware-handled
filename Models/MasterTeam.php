<?php

namespace Models;

use Models\Model;

class MasterTeam extends Model
{
    protected static $table = 'master_teams';

    public static function getMatches($connection)
    {
        $sql = "SELECT mt.id as master_team_id, t.id as team_id, COALESCE(mt.name, t.name) AS name, t.provider_id, t.sport_id
        FROM master_teams as mt
        JOIN team_groups as tg ON mt.id = tg.master_team_id
        JOIN teams as t ON t.id = tg.team_id";

        return $connection->query($sql);
    }
}