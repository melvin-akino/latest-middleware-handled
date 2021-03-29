<?php

namespace Models;

use Models\Model;

class MasterEventMarket extends Model
{
    protected static $table = 'master_event_markets';

    public static function checkIfMemUIDExists($connection, $memUID)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE master_event_market_unique_id = '{$memUID}' LIMIT 1";
        return $connection->query($sql);
    }

    public static function deleteMasterEventMarketByMasterEventId($connection, $masterEventId)
    {
        $sql = "DELETE FROM " . static::$table . " WHERE master_event_id = {$masterEventId}";
        return $connection->query($sql);
    }
}