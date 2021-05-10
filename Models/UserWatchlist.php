<?php

namespace Models;

use Models\Model;

class UserWatchlist extends Model
{
    protected static $table = 'user_watchlist';

    public static function getUserWatchlists($connection)
    {
        $sql = "SELECT master_event_id FROM " . self::$table;
        return $connection->query($sql);
    }
}