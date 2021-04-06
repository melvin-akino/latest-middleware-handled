<?php

namespace Models;

use Models\Model;

class LeagueGroup extends Model
{
    protected static $table = 'league_groups';

    public static function checkIfMatched($connection, $leagueId)
    {
        $sql = "SELECT * FROM " . self::$table . "  WHERE league_id = '{$leagueId}'";
        return $connection->query($sql);
    }

    public static function checkMatchedMasterLeague($connection, $masterLeagueId, $leagueId)
    {
        $sql = "SELECT * FROM " . self::$table . "  WHERE master_league_id = '{$masterLeagueId}' AND league_id = '{$leagueId}'";

        return $connection->query($sql);
    }
}