<?php

namespace Models;

use Models\Model;

class EventGroup extends Model
{
    protected static $table = 'event_groups';

    public static function checkIfMatched($connection, $eventId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE event_id = '{$eventId}'";
        return $connection->query($sql);
    }

    public static function getEventsByMasterEventId($connection, $masterEventId)
    {
        $sql = "SELECT * FROM " . static::$table . " 
                JOIN events ON events.id = " . static::$table . ".event_id WHERE " . static::$table . ".master_event_id = '{$masterEventId}' AND deleted_at is null";

                // var_dump($sql);
        return $connection->query($sql);
    }

    public static function getDataByEventId($connection, $eventId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE event_id = '{$eventId}' LIMIT 1";
        // var_dump($sql);
        return $connection->query($sql);
    }

    public static function matchEvent($connection, $params)
    {
		$sql = "INSERT INTO " . self::$table . "(";
		$array2 = self::_index2string($params);

		$sql .= self::_field($array2);

		$sql .= ") VALUES (";
		
        $sql .= self::_sfield($params);
		
        $sql .= ")";
        echo $sql . "\n";
		return $connection->query($sql);
	}

    private static function _field($array)
	{
		$str = implode(", ", $array);
		return $str;
	}

    private static function _sfield($array)
    {
		$str = "";
		for ($i = 0; $i < count($array); $i++){
			$str .= "'" . addslashes($array[key($array)]) . "'";
			next($array);
			if ($i < count($array) - 1) {
				$str .= ", ";
			}
		}
		return $str;
	}

    private static function _index2string($array)
    {
		$array2 = array();
		for ($i=0; $i < count($array); $i++) {
			$array2[key($array)] = key($array);
			next($array);
		}
		return $array2;
	}
}