<?php
namespace M1ke\Sql;

use Aura\Sql\ExtendedPdo as AuraPdo;
use Aura\Sql\ExtendedPdoInterface;
use Aura\Sql\Exception;

/**
 *
 * Expands Aura's ExtendedPdo to add database manipulation helpers
 *
 * @package M1ke.Sql
 *
 */
class ExtendedPdo extends AuraPdo implements ExtendedPdoInterface
{
	public function __construct($db, $user, $pass, array $options = [], $type = 'mysql', $server = 'localhost'){
		$dsn = $type.':host='.$server.';dbname='.$db;
		parent::__construct($dsn, $user, $pass, $options);
	}

	/**
	 *
	 * Fetches an associative array of rows from the database; the rows
	 * are returned as associative arrays, and the array of rows is keyed
	 * on the first column of each row.
	 *
	 * N.b.: If multiple rows have the same first column value, the last
	 * row with that value will override earlier rows.
	 *
	 * @param string $statement The SQL statement to prepare and execute.
	 *
	 * @param array $values Values to bind to the query.
	 *
	 * @param callable $callable A callable to be applied to each of the rows
	 * to be returned.
	 *
	 * @param string $key_field The field to use as a key.
	 *
	 * @return array
	 *
	 */
	public function fetchAssoc($query, array $values = [], ...$args){
		$statement = $this->perform($query, $values);

		foreach ($args as $arg){
			if (is_callable($arg)){
				$callable = $arg;
			}
			else {
				$key_field = $arg;
			}
		}

		$data = [];
		while ($row = $statement->fetch(self::FETCH_ASSOC)){
			if ($callable){
				$row = call_user_func($callable, $row);
			}
			$key = $key_field ? $row[$key_field] : current($row);
			$data[$key] = $row;
		}
		return $data;
	}

    /**
     *
     * Fetches one field from one row of the database as a scalar value.
     *
     * @param string $statement The SQL statement to prepare and execute.
     *
     * @param array $values Values to bind to the query.
     *
     * @return string
     *
     */
	public function fetchSingle(...$args){
		$data = $this->fetchOne(...$args);
		$value = is_array($data) ? reset($data) : [];
		return $value;
	}

	/**
	 *
	 * Performs a query and then returns the last inserted ID
	 *
	 * @param string $query The SQL statement to prepare and execute.
	 *
	 * @param array $values Values to bind to the query.
	 *
	 * @return integer
	 *
	 */
	public function performId($query, $values){
		$this->perform($query, $values);
		return $this->lastInsertId();
	}

	public function queryInsert($table_name, Array $values, Array $exclude_keys = []){
		$query = "INSERT INTO `$table_name` ".self::makeQueryInsert($values, $exclude_keys);
		return $this->performId($query, $values);
	}

	public function queryUpdate($table_name, $query, Array $values, Array $exclude_keys = []){
		$query_update = self::makeQueryInsert($values, $exclude_keys);
		$query = str_replace("SET :values", "SET $query_update", $query);
		$query = "UPDATE $table_name $query";
		return $this->fetchAffected($query, $values);
	}

	private static function makeQueryInsert(Array $values, Array $exclude_keys = []){
		list($fields, $vals) = self::makeQueryValues('insert', $values, $exclude_keys);
		$vals = "(".implode(',', $vals).")";
		return "(".implode(',', $fields).") VALUES $vals";
	}

	public static function makeQueryValues($type, Array $values, Array $exclude_keys = []){
		foreach ($values as $key => $val){
			$value_isnt_false = $val!==false;
			$key_has_no_dash = !in_string('-', $key);
			$key_isnt_excluded = !in_array($key, $exclude_keys);
			$value_isnt_array = !is_array($val);
			if ($value_isnt_false && $key_has_no_dash && $key_isnt_excluded && $value_isnt_array){
				$val = is_null($val) ? "''" : ":$key";
				$fields[] = $type==='update' ? "`$key`=$val" : "`$key`";
				$vals[] = $val;
			}
		}
		return [$fields, $vals];
	}

	private static function makeQueryUpdate(Array $values, Array $exclude_keys = []){
		list($query) = self::makeQueryValues('update', $values, $exclude_keys);
		return implode(',', $query);
	}
}