<?php

namespace Models;

use Models\Model;

class League extends Model
{
    protected static $table = 'leagues';

    public static function getActiveLeagues($connection)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE deleted_at is null";
        return $connection->query($sql);
    }

    public static function getLeague($connection, $leagueId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE id = $leagueId";
        return $connection->query($sql);
    }

    public static function getUnmatchedLeagues($connection)
    {
        $sql = "SELECT * FROM " . self::$table . " as l
                WHERE NOT EXISTS (SELECT null FROM league_groups as lg WHERE lg.league_id = l.id)";
        return $connection->query($sql);
    }

    public static function getMajorLeagues($connection)
    {
        $sql = "SELECT master_league_id FROM " . self::$table . 
        " JOIN league_groups on league_groups.league_id = ".self::$table.".id 
        JOIN master_leagues on master_leagues.id = league_groups.master_league_id WHERE is_priority=true AND master_leagues.deleted_at is null";
        return $connection->query($sql);
    }
}