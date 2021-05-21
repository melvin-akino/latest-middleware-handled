<?php

namespace Models;

class UserBet extends Model
{
    protected static $table = 'user_bets';

    public static function updateById($connection, $userBetId, $arrayParams)
    {
        self::update($connection, $arrayParams, [
            'id' => $userBetId
        ]);
        return $connection->query($sql);
    }
}