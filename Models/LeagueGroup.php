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
}