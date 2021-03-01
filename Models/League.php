<?php

namespace Models;

use Models\Model;

Class League extends Model
{
    protected static $table = 'leagues';

    public static function getActiveLeagues($connection)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE deleted_at is null";
        return $connection->query($sql);
    }
}