<?php
namespace M1ke\Sql;

use Aura\Sql\ExtendedPdo as AuraPdo;

use PDOException;
use PDOStatement;

/**
 *
 * Expands Aura's ExtendedPdo to add database manipulation helpers
 *
 * @package M1ke.Sql
 *
 */
class ExtendedPdo extends AuraPdo{
    /** @internal */
    public const KEY_COLLISION = '____';

    /**
     * ExtendedPdo constructor.
     * @param string $db
     * @param string $user
     * @param string $pass
     * @param array $options
     * @param string $charset
     * @param string $type
     * @param string $server
     */
	public function __construct(string $db, string $user, string $pass, array $options = [], string $charset = 'utf8', string $type = 'mysql', string $server = 'localhost'){
		$dsn = "$type:host={$server};dbname={$db}";

		if (!empty($charset)){
			$dsn .= ";charset={$charset}";
		}

		parent::__construct($dsn, $user, $pass, $options);
	}


    public function getDsn(): string {
        return $this->args[0];
    }

	/**
	 * @param array<string, scalar> $values
	 * @param list<string> $exclude_keys
	 * @return list<string>
	 */
	public static function excludeKeys(array $values, array $exclude_keys):array{
		$include_keys = array_keys($values);
		foreach ($include_keys as $n => $key){
			if (in_array($key, $exclude_keys)){
				unset($include_keys[$n]);
			}
		}

		return $include_keys;
	}

    /**
     * Fetches a sequential array of rows from the database; the rows
     * are returned as associative arrays.
     *
     * @param string $statement The SQL statement to prepare and execute.
     * @param array<string, ?scalar|list<scalar>> $values
     * @param callable|null $callable $callable A callable to be applied to each of the rows to be returned.
     *
     * @return array
     * @throws Exception
     */
    public function fetchAll($statement, array $values = [], callable $callable = null ):array{
        return $this->fetchAllWithCallable(self::FETCH_ASSOC, $statement, $values, $callable);
    }

    /**
     * @param int $fetch_type
     * @param string $statement
     * @param array<string, ?scalar|list<scalar>> $values
     * @param callable|null $callable
     * @return array
     * @throws Exception
     */
	protected function fetchAllWithCallable(int $fetch_type, string $statement, array $values = [], callable $callable = null):array{
        $sth = $this->perform($statement, $values);
        if ($fetch_type === self::FETCH_COLUMN) {
            $data = $sth->fetchAll($fetch_type, 0);
        } else {
            $data = $sth->fetchAll($fetch_type);
        }
        if ($callable) {
            foreach ($data as $key => $row) {
                $data[$key] = call_user_func($callable, $row);
            }
        }
		return is_array($data) ? $data : [];
	}


    /**
     * Fetches the first column of rows as a sequential array.
     * @param string $statement The SQL statement to prepare and execute.
     * @param array<string, ?scalar|list<scalar>> $values
     * @param callable|null $callable $callable A callable to be applied to each of the rows
     * to be returned.
     *
     * @return array
     * @throws Exception
     */
    public function fetchCol($statement, array $values = [], callable $callable = null):array{
        return $this->fetchAllWithCallable(self::FETCH_COLUMN, $statement, $values, $callable);
    }

	/**
	 * @param string $statement
	 * @param array<string, ?scalar|list<scalar>> $values
	 * @return array
	 */
	public function fetchOne($statement, array $values = []): array {
		$return = parent::fetchOne($statement, $values);

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
	 * @param string $statement
	 * @param array<string, ?scalar|list<scalar>> $values
	 * @param ?callable $callable
	 * @param ?string $key_field
	 * @return array
	 * @throws Exception
	 */
	public function fetchAssoc($statement, array $values = [], callable $callable = null, string $key_field = ''): array
    {
		$stmt = $this->perform($statement, $values);

		$data = [];
		while ($row = $stmt->fetch(self::FETCH_ASSOC)){
			if ($callable){
				$row = call_user_func($callable, $row);
			}
			$key = !empty($key_field) ? $row[$key_field] : current($row);
			$data[$key] = $row;
		}

		return $data;
	}

    /**
     * @param $statement
     * @param array $values
     * @param callable|null $callable
     * @return array|false
     * @throws Exception
     */
    public function fetchPairs(
        $statement,
        array $values = [],
        callable $callable = null
    ) {
        $sth = $this->perform($statement, $values);
        if ($callable) {
            $data = [];
            while ($row = $sth->fetch(self::FETCH_NUM)) {
                // apply the callback first so the key can be modified
                $row = call_user_func($callable, $row);
                // now retain the data
                $data[$row[0]] = $row[1];
            }
        } else {
            $data = $sth->fetchAll(self::FETCH_KEY_PAIR);
        }
        return $data;
    }

	/**
	 * Fetches one field from one row of the database as a scalar value.
	 *
	 * @param string $statement
	 * @param array<string, ?scalar|list<scalar>> $values
	 *
	 * @return string|false
	 */
	public function fetchField(string $statement, array $values = []){
		$data = $this->fetchOne($statement, $values);

		return self::fetchFieldReturn($data);
	}

	/**
	 * @param ?array $data
	 * @return mixed|string
	 */
	public static function fetchFieldReturn(?array $data){
		return (!empty($data)) ? reset($data) : '';
	}

    /**
     *
     * Performs a query and then returns the last inserted ID
     *
     * @param string $query The SQL statement to prepare and execute.
     * @param array<string, ?scalar> $values
     * @return string|false
     *
     * @throws Exception
     */
	public function performId(string $query, array $values = []){
		$this->perform($query, $values);

		return $this->lastInsertId();
	}

	/**
	 * @param string $table_name
	 * @param array<string, scalar> $vals
	 * @param list<string> $include_keys
	 * @return string|false
	 * @throws Exception
	 */
	public function insert(string $table_name, array $vals, array $include_keys){
		$query_values = self::makeQueryInsert($vals, $include_keys);
		/** @noinspection SqlResolve */
		$query = "INSERT INTO `$table_name` " . $query_values->query;

		return $this->performId($query, $query_values->values);
	}

    /**
     * @param string $table_name
     * @param array<string, scalar> $values
     * @param list<string> $exclude_keys
     * @return string|false
     * @throws Exception
     */
	public function insertExc(string $table_name, array $values, array $exclude_keys = []){
		$include_keys = self::excludeKeys($values, $exclude_keys);

		return $this->insert($table_name, $values, $include_keys);
	}

	/**
	 * @param string $table_name
	 * @param string|array<string, scalar> $where
	 * @param array<string, scalar> $values
	 * @param list<string> $include_keys
	 * @return int
	 */
	public function update(string $table_name, $where, array $values, array $include_keys):int{
		$query_values = self::makeQueryUpdate($table_name, $where, $values, $include_keys);

		return $this->fetchAffected($query_values->query, $query_values->values);
	}

	/**
	 * @param string $table_name
	 * @param string|array<string, scalar> $where
	 * @param array<string, scalar> $values
	 * @param list<string> $include_keys
	 * @return int
	 */
	public function updateOne(string $table_name, $where, array $values, array $include_keys):int{
		$query_values = self::makeQueryUpdate($table_name, $where, $values, $include_keys);

		$query = $query_values->query;
		$query .= " LIMIT 1";

		return $this->fetchAffected($query, $query_values->values);
	}

	/**
	 * @param string $table_name
	 * @param array|string $where
	 * @param array<string, scalar> $values
	 * @param list<string> $exclude_keys
	 * @return int
	 */
	public function updateExc(string $table_name, $where, array $values, array $exclude_keys = []):int{
		$include_keys = self::excludeKeys($values, $exclude_keys);

		return $this->update($table_name, $where, $values, $include_keys);
	}

	/**
	 * @param string $table_name
	 * @param array|string $where
	 * @param array<string, scalar> $values
	 * @param list<string> $exclude_keys
	 * @return int
	 */
	public function updateOneExc(string $table_name, $where, array $values, array $exclude_keys = []):int{
		$include_keys = self::excludeKeys($values, $exclude_keys);

		return $this->updateOne($table_name, $where, $values, $include_keys);
	}

	/**
	 * @param string $table_name
	 * @param string|array<string, scalar> $where
	 * @param array<string, scalar> $vals
	 * @param list<string> $include_keys
	 * @return ExtendedPdoQueryValues
	 */
	public static function makeQueryUpdate(string $table_name, $where, array $vals, array $include_keys):ExtendedPdoQueryValues{
		$query_values = self::makeQueryUpdateComponents($vals, $include_keys);
		$values = $query_values->values;

		$where_query = self::whereQuery($where, $values);

		if (is_array($where)){
			$where = self::makeQueryUpdateWhere($where, $values);
			$values = array_merge($values, $where);
		}
		$table_name = "`$table_name`";
		$query = "UPDATE $table_name SET {$query_values->query} WHERE $where_query";

		return new ExtendedPdoQueryValues($query, $values);
	}

	/**
	 * @param array<string, scalar> $where
	 * @param array<string, ?scalar> $values
	 * @return array<string, ?scalar>
	 */
	public static function makeQueryUpdateWhere(array $where, array $values):array{
		$where_copy = [];
		foreach ($where as $key => $val){
			if (isset($values[$key])){
				$key = $key . self::KEY_COLLISION;
			}
			if ($val===false){
				$val = null;
			}
			$where_copy[$key] = $val;
		}

		return $where_copy;
	}

	/**
	 * @param string|array<string, scalar> $where
	 * @param array<string, ?scalar> $values
	 * @return string
	 */
	public static function whereQuery($where, array $values = []):string{
		if (is_array($where)){
			$where_keys = [];
			foreach ($where as $key => $val){
				$operator = '=';
				$param_key = isset($values[$key]) ? ($key . self::KEY_COLLISION) : $key;
				$where_keys[] = "`$key` $operator :$param_key";
			}
			$where_query = implode(' and ', $where_keys);
		}
		else {
			$where_query = $where;
		}

		return $where_query;
	}

    /**
     * @param array<string, scalar> $where
     * @param array<string, scalar> $operators
     * @return string
     */
	public static function whereQueryOperator(array $where, array $operators):string{
		$where_keys = [];
		foreach ($where as $key => $val){
			$operator = '=';
			if (!empty($operators[$key])){
				$operator = $operators[$key];
			}

			[$key, $pfx] = self::splitPrefix($key);

			$where_keys[] = "$pfx`$key` $operator :$key";
		}

		return implode(' and ', $where_keys);
	}

	/**
	 * @param string $key
	 * @return array{non-empty-string, string}
	 */
	protected static function splitPrefix(string $key): array{
		$parts = explode('.', $key);
		if (count($parts)>1){
			$pfx = $parts[0].'.';
			$key = $parts[1];
		}
		else {
			$pfx = '';
			$key = $parts[0];
		}
		return [$key, $pfx];
	}

	/**
	 * @param array<string, scalar> $values
	 * @param list<string> $include_keys
	 * @return ExtendedPdoQueryValues
	 */
	public static function makeQueryInsert(array $values, array $include_keys):ExtendedPdoQueryValues{
		$query_placeholders = self::makeQueryValues('insert', $values, $include_keys);
		$query = "(" . implode(', ', $query_placeholders->fields) . ") VALUES (" . implode(',', $query_placeholders->placeholders) . ")";

		return new ExtendedPdoQueryValues($query, $query_placeholders->vals);
	}

	/**
	 * @param array<string, ?scalar> $values
	 * @param list<string> $include_keys
	 * @return ExtendedPdoQueryValues
	 */
	public static function makeQueryUpdateComponents(array $values, array $include_keys):ExtendedPdoQueryValues{
		$query_placeholders = self::makeQueryValues('update', $values, $include_keys);
		$query = implode(', ', $query_placeholders->fields);

		return new ExtendedPdoQueryValues($query, $query_placeholders->vals);
	}

	/**
	 * @param string $type
	 * @type 'insert'|'update' $type
	 * @param array<string, ?scalar> $values
	 * @param list<string> $include_keys
	 * @return ExtendedPdoQueryPlaceholders
	 */
	public static function makeQueryValues(string $type, array $values, array $include_keys):ExtendedPdoQueryPlaceholders{
		$fields = $placeholders = $vals = [];
		$values_keys = array_keys($values);

		foreach ($include_keys as $key){
			$key = trim($key);
			$val = $values[$key]??null;
			$key_is_set = in_array($key, $values_keys);
			$value_isnt_false = $val!==false;
			$key_has_no_dash = strpos($key, '-')===false;
			$value_isnt_array = !is_array($val);

			if ($value_isnt_false && $key_has_no_dash && $value_isnt_array && $key_is_set){
				$placeholder = ":$key";
				$fields[] = $type==='update' ? "`$key` = $placeholder" : "`$key`";
				$placeholders[] = $placeholder;
				$vals[$key] = !is_null($val) ? $val : '';
			}
		}

		return new ExtendedPdoQueryPlaceholders($fields, $placeholders, $vals);
	}

	/**
	 * @param string $statement
	 * @param  array<string, ?scalar|list<scalar>> $values
	 * @return PDOStatement
	 * @throws Exception
	 */
	public function perform($statement, array $values = []):PDOStatement{
		$sth = $this->prepareWithValues($statement, $values);
        $this->profiler->start(__FUNCTION__);
		try {
			$sth->execute();
		}
		catch (PDOException $e) {
			throw new Exception($e->getMessage(), $sth->queryString);
		}
		$this->profiler->finish($statement, $values);

		return $sth;
	}

	// We actually might not need this, as ExtendedPdo can fill
	//  in arrays for us as part of its "prepare" process
	public static function prepareList(array $arr = []): string {
		$count = count($arr);
		$list = [];
		for ($n = 0; $n<$count; $n++){
			$list[] = '?';
		}

		return implode(',', $list);
	}

	/**
	 * We don't typehint the $where value because it can be a manually typed
	 * string for things such as > < between etc.
	 * @param string $table
	 * @param string|array<string, scalar> $where
	 * @param string $type
	 * @psalm-type 'one'|'assoc'|'all'|'field' $type
	 * @param string|list<string> $fields
	 * @return array|string
	 * @psalm-return ($type is 'field' ? string : array)
	 */
	public function selectFrom(string $table, $where, string $type = 'one', $fields = "*"){
		$query = self::selectFromQuery($table, $where, $fields);
		$func = 'fetch' . ucfirst($type);
		// if we had a string $where, pass on an empty array as where
		if (!is_array($where)){
			$where = [];
		}
		else {
			$where = $this->removeFalseWhere($where);
		}

		return $this->$func($query, $where);
	}

	/**
	 * @param string $table
	 * @param string|array<string, scalar> $where
	 * @param string|list<string> $fields
	 * @return string
	 */
	public static function selectFromQuery(string $table, $where, $fields):string{
		$where_query = self::whereQuery($where);

		$fields = self::fieldFormat($fields);

		/** @noinspection SqlResolve */
		return "SELECT $fields FROM `$table`".(!empty($where_query) ? " WHERE $where_query" : '');
	}

	/**
	 * @param string|list<string> $fields
	 * @return string
	 */
	private static function fieldFormat($fields):string{
		if (!is_array($fields)){
			$fields = explode(',', $fields);
		}
		foreach ($fields as &$field){
			$field = trim($field);
			if (strpos($field, '*')===false){
				[$key, $pfx] = self::splitPrefix($field);
				$field = "$pfx`$key`";
			}
		}

		return implode(',', $fields);
	}

	/**
	 * @param string $table
	 * @param string|array<string, scalar> $where
	 * @return int
	 */
	public function selectCount(string $table, $where):int{
		return (int) $this->selectFrom($table, $where, 'field', ['count(*)']);
	}

	/**
	 * @param string $table
	 * @param string|array<string, scalar> $where
	 * @param string|list<string> $fields
	 * @return array
	 */
	public function selectOne(string $table, $where, $fields = "*"){
		return $this->selectFrom($table, $where, 'one', $fields);
	}

	/**
	 * @param string $table
	 * @param string|array<string, scalar> $where
	 * @param array|string $fields
	 * @return list<array>
	 */
	public function selectAll(string $table, $where, $fields = "*"){
		return $this->selectFrom($table, $where, 'all', $fields);
	}

	/**
	 * @param string $table
	 * @param string|array<string, scalar> $where
	 * @param array|string $field
	 * @return string
	 */
	public function selectField(string $table, $where, $field):string{
		return $this->selectFrom($table, $where, 'field', $field);
	}

	/**
	 * @param string $table
	 * @param string|array<string, scalar> $where
	 * @param ?int $limit
	 * @return int
	 * @throws Exception
	 */
	public function delete(string $table, $where, ?int $limit = null):int{
		$query_values = self::deleteQuery($table, $where, $limit);

		return $this->fetchAffected($query_values->query, $query_values->values);
	}

	/**
	 * @param string $table
	 * @param string|array<string, scalar> $where
	 * @return int
	 * @throws Exception
	 */
	public function deleteOne(string $table, $where):int{
		return $this->delete($table, $where, 1);
	}

	/**
	 * @param string $table
	 * @param string|array<string, scalar> $where
	 * @param ?int $limit
	 * @return ExtendedPdoQueryValues
	 * @throws Exception
	 */
	public static function deleteQuery(string $table, $where, ?int $limit = null):ExtendedPdoQueryValues{
		$where_query = self::whereQuery($where);
		/** @noinspection SqlResolve */
		$query = "DELETE FROM `$table` WHERE $where_query";
		if (empty($where_query)){
			throw new Exception('Delete commands must contain a WHERE component.', $query);
		}
		if (!empty($limit)){
			if (is_array($where)){
				$query .= " LIMIT :_limit";
				$where['_limit'] = $limit;
			}
			else {
				$query .= " LIMIT $limit";
			}
		}

		return new ExtendedPdoQueryValues($query, is_array($where) ? $where : []);
	}

	/**
	 * @template T of array<string, scalar>
	 * @param T $where
	 * @return T
	 */
	private function removeFalseWhere($where){
		foreach ($where as &$val){
			if ($val===false){
				$val = null;
			}
		}

		return $where;
	}
}

/**
 * Class ExtendedPdoQueryPlaceholders
 * @package M1ke\Sql
 *
 * Acts as a hintable return type for makeQueryValues
 */
class ExtendedPdoQueryPlaceholders {
    /** @var list<string>  */
	public array $fields;
    /** @var list<string>  */
	public array $placeholders;
    /** @var array<string, ?scalar> */
	public array $vals;

	/**
	 * @param list<string> $fields
	 * @param list<string> $placeholders
	 * @param array<string, ?scalar> $vals
	 */
	public function __construct(array $fields, array $placeholders, array $vals){
		$this->fields = $fields;
		$this->placeholders = $placeholders;
		$this->vals = $vals;
	}
}

/**
 * Class ExtendedPdoQueryValues
 * @package M1ke\Sql
 *
 * Acts as a hintable return type for query/value mixed generation
 */
class ExtendedPdoQueryValues {

	public string $query;
    /** @var array<string, ?scalar> */
	public array $values = [];

	/**
	 * ExtendedPdoQueryValues constructor.
	 * @param string $query
	 * @param array<string, ?scalar> $values
	 */
	public function __construct(string $query, array $values = []){
		$this->query = $query;
		$this->values = $values;
	}
}
