<?php

namespace Models;

Class Sport
{
    protected static $table = 'sports';

    public static function getActiveSports($connection)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE is_enabled = true";
        return $connection->query($sql);
        
    }
}