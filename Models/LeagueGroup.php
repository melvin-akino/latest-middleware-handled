<?php

namespace Models;

use Models\Model;

Class LeagueGroup extends Model
{
    protected static $table = 'league_groups';

    public static function getLeaguesData($connection)
    {
        $sql = "SELECT lg.*, name, sport_id, provider_id FROM " . self::$table . " as lg
                JOIN leagues as l ON lg.league_id = l.id WHERE l.deleted_at is null";
        return $connection->query($sql);
    }
}