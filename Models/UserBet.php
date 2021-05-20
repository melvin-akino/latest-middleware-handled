<?php

namespace Models;

class UserBet
{
    private static $table = 'user_bets';

    public static function updateByBetIdNumber($connection, $userBetId, $arrayParams)
    {
        $sql    = "UPDATE " . self::$table . " SET ";
        $params = [];
        foreach ($arrayParams as $key => $value) {
            $params[] = "{$key} = '{$value}'";
        }
        $sql .= implode(', ', $params);
        $sql .= "WHERE id LIKE '%{$userBetId}'";
        return $connection->query($sql);
    }
}