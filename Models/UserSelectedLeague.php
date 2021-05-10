<?php

namespace Models;

use Models\Model;

class UserSelectedLeague extends Model
{
    protected static $table = 'user_selected_leagues';

    public static function getUserSelectedLeagues($connection)
    {
        $sql = "SELECT master_league_id FROM " . self::$table;
        return $connection->query($sql);
    }
}