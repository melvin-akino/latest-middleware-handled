<?php

namespace Models;

Class Order
{
    private static $table = 'orders';

    public static function getDataByBetId($connection, $providerBetId)
    {
        $sql = "SELECT o.*, u.currency_id, u.uuid, c.code FROM " . self::$table . " as o
                JOIN users as u ON u.id = o.user_id
                JOIN currency as c ON c.id = u.currency_id
                WHERE bet_id LIKE '%{$providerBetId}' ORDER BY id LIMIT 1";
        return $connection->query($sql);
    }

    public static function getActiveOrders($connection)
    {
        $sql = "SELECT o.id, o.status, o.created_at, o.bet_id, o.order_expiry, pa.username, u.currency_id as user_currency_id FROM " . self::$table . " as o
                JOIN provider_accounts as pa ON pa.id = o.provider_account_id
                JOIN users as u ON u.id = o.user_id
                WHERE settled_date is null";
        return $connection->query($sql);
    }

    public static function updateByBetIdNumber($connection, $providerBetId, $arrayParams)
    {
        $sql = "UPDATE " . self::$table . " SET ";
        $params = [];
        foreach ($arrayParams as $key => $value) {
            $params[] = "{$key} = '{$value}'";
        }
        $sql .= implode(', ', $params);
        $sql .= "WHERE bet_id LIKE '%{$providerBetId}'";
        echo $sql . "\n";
        return $connection->query($sql);
    }
}