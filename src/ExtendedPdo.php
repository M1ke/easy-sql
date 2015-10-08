<?php
namespace M1ke\Sql;

use Aura\Sql\ExtendedPdo as AuraPdo;
use Aura\Sql\ExtendedPdoInterface;

use PDOException;

/**
 *
 * Expands Aura's ExtendedPdo to add database manipulation helpers
 *
 * @package M1ke.Sql
 *
 */
class ExtendedPdo extends AuraPdo implements ExtendedPdoInterface
{
	public static $where_key_collision = '____';

	public function __construct($db, $user, $pass, array $options = [], $type = 'mysql', $server = 'localhost'){
		$dsn = $type.':host='.$server.';dbname='.$db;
		parent::__construct($dsn, $user, $pass, $options);
	}

	public static function excludeKeys(Array $values, Array $exclude_keys){
		$include_keys = array_keys($values);
		foreach ($include_keys as $n => $key){
			if (in_array($key, $exclude_keys)){
				unset($include_keys[$n]);
			}
		}
		return $include_keys;
	}

	protected function fetchAllWithCallable($fetch_type, $statement, array $values = array(), $callable = null){
		$args = func_get_args();
		$return = parent::fetchAllWithCallable(...$args);
		return is_array($return) ? $return : [];
	}

	public function fetchOne($statement, array $values = []){
		$args = func_get_args();
		$return = parent::fetchOne(...$args);
		return is_array($return) ? $return : [];
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
	public function fetchField(...$args){
		$data = $this->fetchOne(...$args);
		$value = is_array($data) ? reset($data) : '';
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
		$this->perform($query, $values, true);
		return $this->lastInsertId();
	}

	public function queryInsert($table_name, Array $values, Array $include_keys){
		list($query, $values) = self::makeQueryInsert($values, $include_keys);
		$query = "INSERT INTO `$table_name` ".$query;
		return $this->performId($query, $values);
	}

	public function queryInsertExc($table_name, Array $values, Array $exclude_keys = []){
		$include_keys = self::excludeKeys($values, $exclude_keys);
		return $this->queryInsert($table_name, $values, $include_keys);
	}

	public function queryUpdate($table_name, $where, Array $values, Array $include_keys){
		list($query, $values) = self::makeQueryUpdate($table_name, $where, $values, $include_keys);
		return $this->fetchAffected($query, $values);
	}

	public static function makeQueryUpdate($table_name, $where, Array $values, Array $include_keys){
		list($query_update, $values) = self::makeQueryUpdateComponents($values, $include_keys);
		$where_query = self::whereQuery($where, $values);
		if (is_array($where)){
			$where = self::makeQueryUpdateWhere($where, $values);
			$values = array_merge($values, $where);
		}
		$query = "UPDATE $table_name SET $query_update WHERE $where_query";
		return [$query, $values];
	}

	public static function makeQueryUpdateWhere(array $where, array $values){
		$where_copy = [];
		foreach ($where as $key => $val){
			if (isset($values[$key])){
				$key = $key.self::$where_key_collision;
			}
			$where_copy[$key] = $val;
		}
		return $where_copy;
	}

	public static function whereQuery($where, $values = []){
		if (is_array($where)){
			$where_keys = [];
			foreach ($where as $key => $val){
				$operator = '=';
				$param_key = isset($values[$key]) ? ($key . self::$where_key_collision) : $key;
				$where_keys[] = "$key $operator :$param_key";
			}
			$where_query = implode(' and ', $where_keys);
		}
		else {
			$where_query = $where;
		}
		return $where_query;
	}

	public static function whereQueryOperator(Array $where, Array $operators){
		$where_keys = [];
		foreach ($where as $key => $val){
			$operator = '=';
			if (!empty($operators[$key])){
				$operator = $operators[$key];
			}
			$where_keys[] = "$key $operator :$key";
		}
		$where_query = implode(' and ', $where_keys);
		return $where_query;
	}

	public function queryUpdateExc($table_name, $where, Array $values, Array $exclude_keys = []){
		$include_keys = self::excludeKeys($values, $exclude_keys);
		return $this->queryUpdate($table_name, $where, $values, $include_keys);
	}

	public static function makeQueryInsert(Array $values, Array $include_keys){
		list($fields, $placeholders, $vals) = self::makeQueryValues('insert', $values, $include_keys);
		$query = "(".implode(', ', $fields).") VALUES (".implode(',', $placeholders).")";
		return [$query, $vals];
	}

	public static function makeQueryUpdateComponents(Array $values, Array $include_keys){
		list($fields, $placeholders, $vals) = self::makeQueryValues('update', $values, $include_keys);
		$query = implode(', ', $fields);
		return [$query, $vals];
	}

	public static function makeQueryValues($type, Array $values, Array $include_keys){
		$placeholders = $vals = [];
		foreach ($include_keys as $key){
			$val = $values[$key];
			$value_isnt_false = $val!==false;
			$key_has_no_dash = strpos($key, '-')===false;
			$value_isnt_array = !is_array($val);
			if ($value_isnt_false && $key_has_no_dash && $value_isnt_array){
				$placeholder = ":$key";
				$fields[] = $type==='update' ? "`$key` = $placeholder" : "`$key`";
				$placeholders[] = $placeholder;
				$vals[$key] = !is_null($val) ? $val : '';
			}
		}
		return [$fields, $placeholders, $vals];
	}

	public function perform($statement, array $values = []){
        $sth = $this->prepareWithValues($statement, $values);
        $this->beginProfile(__FUNCTION__);
        try {
        	$sth->execute();
        }
        catch (PDOException $e){
        	throw new Exception($e->getMessage(), $sth->queryString);
        }
        $this->endProfile($statement, $values);
        return $sth;
    }

    // We actually might not need this, as ExtendedPdo can fill
    //  in arrays for us as part of its "prepare" process
	public static function prepareList(Array $arr = []){
		$count = count($arr);
		for ($n=0; $n<$count; $n++){
			$list[] = '?';
		}
		return implode(',', $list);
	}

	public function selectFrom($table, $where, $type='one', $fields = "*"){
		$query = self::selectFromQuery($table, $where, $fields);
		$func = 'fetch'.ucfirst($type);
		$data = $this->$func($query, $where);
		return $data;
	}

	public static function selectFromQuery($table, $where, $fields){
		$where_query = self::whereQuery($where);
		if (is_array($fields)){
			$fields = implode(',', $fields);
		}
		$query = "SELECT $fields FROM `$table` WHERE $where_query";
		return $query;
	}

	public function selectCount($table, $where){
		return $this->selectFrom($table, $where, 'field', ['count(*)']);
	}

	public function selectOne($table, $where, $fields = "*"){
		return $this->selectFrom($table, $where, 'one', $fields);
	}

	public function selectAll($table, $where, $fields = "*"){
		return $this->selectFrom($table, $where, 'all', $fields);
	}

	public function selectField($table, $where, $field){
		return $this->selectFrom($table, $where, 'field', $field);
	}

	public function delete($table, $where, $limit = null){
		list($query, $where) = self::deleteQuery($table, $where, $limit);
		return $this->fetchAffected($query, $where);
	}

	public static function deleteQuery($table, $where, $limit){
		$where_query = self::whereQuery($where);
		$query = "DELETE FROM `$table` WHERE $where_query";
		if (empty($where_query)){
			throw new Exception('Delete commands must contain a WHERE component.', $query);
		}
		if (!empty($limit)){
			$query .= " LIMIT :_limit";
			$where['_limit'] = $limit;
		}
		return [$query, $where];
	}
}
