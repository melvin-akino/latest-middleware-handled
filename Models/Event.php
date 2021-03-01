<?php

namespace Models;

use Models\Model;

Class Event extends Model
{
    protected static $table = 'events';

    public static function getActiveEvents($connection)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE deleted_at is null";
		return $connection->query($sql);
    }
}