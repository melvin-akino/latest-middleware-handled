<?php

namespace Models;

use Models\Model;

class UnmatchedEvent extends Model
{
    protected static $table = 'unmatched_data';

    public static function deleteUnmatched($connection, $dataId)
    {
        $sql = "DELETE FROM " . static::$table . " WHERE type = 'event' AND data_id = {$dataId}";
        return $connection->query($sql);
    }
}