<?php

require __DIR__.'/_bootstrap.php';

use M1ke\Sql\Exception as ExtendedPdoException;
use M1ke\Sql\ExtendedPdo;

class TestBasic extends PHPUnit_Framework_TestCase {
	public function testClassInstantiates(){
		$pdo = new ExtendedPdo($db, $user, $pass);
		$this->assertTrue($pdo instanceof \Aura\Sql\ExtendedPdo);
		$this->assertTrue($pdo instanceof \PDO);
	}

	public function testMakeQueryInsert(){
		$values = [
			'string'=> 'test',
			'not'=> 'nope',
		];
		$include_keys = ['string'];
		list($query, $values) = ExtendedPdo::makeQueryInsert($values, $include_keys);
		$this->assertEquals("(`string`) VALUES (:string)", $query);
		$this->assertEquals([
			'string'=> 'test',
		], $values);
	}

	public function testMakeQueryUpdate(){
		$table_name = 'table';
		$where = ['key'=> 1];
		$values = [
			'string'=> 'test',
			'not'=> 'nope',
		];
		$include_keys = ['string'];
		list($query, $values) = ExtendedPdo::makeQueryUpdate($table_name, $where, $values, $include_keys);
		$this->assertEquals("UPDATE table SET `string` = :string WHERE key = :key", $query);
		$this->assertEquals([
			'string'=> 'test',
			'key'=> 1,
		], $values);
	}

	public function testMakeQueryUpdateWhereString(){
		$table_name = 'table';
		$where = 'key=1';
		$values = [
			'string'=> 'test',
			'not'=> 'nope',
		];
		$include_keys = ['string'];
		list($query, $values) = ExtendedPdo::makeQueryUpdate($table_name, $where, $values, $include_keys);
		$this->assertEquals("UPDATE table SET `string` = :string WHERE key=1", $query);
		$this->assertEquals([
			'string'=> 'test',
		], $values);
	}

	public function testMakeQueryUpdateWhere(){
		$value = 'val';
		$key = 'test';
		$where = [
			$key=> $value,
		];
		$values = $where;
		$where_new = ExtendedPdo::makeQueryUpdateWhere($where, $values);

		$collision_string = ExtendedPdo::$where_key_collision;
		$this->assertEquals([$key.$collision_string=> $value], $where_new);
	}

	public function testMakeQueryUpdateRepeatedField(){
		$table_name = 'table';
		$where = [
			'key'=> 1,
			'string'=>'old string'
		];
		$values = [
			'string'=> 'test',
		];
		$include_keys = ['string'];
		$collision_string = ExtendedPdo::$where_key_collision;
		list($query, $values) = ExtendedPdo::makeQueryUpdate($table_name, $where, $values, $include_keys);
		$this->assertEquals("UPDATE table SET `string` = :string WHERE key = :key and string = :string$collision_string", $query);
		$this->assertEquals([
			'string'=> 'test',
			'key'=> 1,
			'string'.$collision_string=> 'old string',
		], $values);
	}

	public function testWhereQuery(){
		$where = [
			'id'=> 1,
			'string'=> 'test',
		];
		$where_query = ExtendedPdo::whereQuery($where);
		$this->assertEquals("id = :id and string = :string", $where_query);
	}

	public function testWhereQueryString(){
		$where = "id = :id and string = :string";
		$where_query = ExtendedPdo::whereQuery($where);
		$this->assertEquals("id = :id and string = :string", $where_query);
	}

	public function testWhereQueryOperator(){
		$where = [
			'id'=> 1,
			'date'=> '2015-07-30',
			'number'=> 6,
			'bad'=> 'test'
		];
		$where_query = ExtendedPdo::whereQueryOperator($where, ['date'=> '>=', 'number'=> '<', 'unset'=> '<>' , 'bad'=> '']);
		$this->assertEquals("id = :id and date >= :date and number < :number and bad = :bad", $where_query);
	}

	public function testMakeQueryValuesInsert(){
		$values = [
			'id'=> 1,
			'string'=> 'test',
			'empty'=> '',
			'false'=> false,
			'null'=> null,
			'ignored'=> 'rawr',
		];
		$include_keys = ['id', 'string', 'empty', 'false', 'null'];
		list($fields, $placeholders, $vals) = ExtendedPdo::makeQueryValues('insert', $values, $include_keys);
		$this->assertEquals(['`id`', '`string`', '`empty`', '`null`'], $fields);
		$this->assertEquals([':id', ':string', ':empty', ':null'], $placeholders);
		$this->assertEquals([
			'id'=> 1,
			'string'=> 'test',
			'empty'=> '',
			'null'=> '',
		], $vals);
	}

	public function testMakeQueryValuesUpdate(){
		$values = [
			'id'=> 1,
			'string'=> 'test',
			'empty'=> '',
			'false'=> false,
			'null'=> null,
			'ignored'=> 'rawr',
		];
		$include_keys = ['id', 'string', 'empty', 'false', 'null'];
		list($fields, $placeholders, $vals) = ExtendedPdo::makeQueryValues('update', $values, $include_keys);
		$this->assertEquals(['`id` = :id', '`string` = :string', '`empty` = :empty', '`null` = :null'], $fields);
		// Placeholders should be the same as "testMakeQueryValuesInsert" AND are irrelevant for update
		// Vals should be the same as "testMakeQueryValuesInsert"
		$this->assertEquals([
			'id'=> 1,
			'string'=> 'test',
			'empty'=> '',
			'null'=> '',
		], $vals);
	}

	public function testSelectFromQuery(){
		$table = 'test';
		$where = [
			'id' => 1,
		];
		$fields = '*';
		$query = ExtendedPdo::selectFromQuery($table, $where, $fields);
		$this->assertEquals("SELECT * FROM `test` WHERE id = :id", $query);
	}

	public function testSelectFromQueryFieldArray(){
		$table = 'test';
		$where = [
			'id' => 1,
		];
		$fields = ['id', 'string'];
		$query = ExtendedPdo::selectFromQuery($table, $where, $fields);
		$this->assertEquals("SELECT id,string FROM `test` WHERE id = :id", $query);
	}

	public function testSelectFromQueryWhereString(){
		$table = 'test';
		$where = "id = 1";
		$fields = 'string';
		$query = ExtendedPdo::selectFromQuery($table, $where, $fields);
		$this->assertEquals("SELECT string FROM `test` WHERE id = 1", $query);
	}

	public function testExcludeKeys(){
		$values = [
			'id'=> 1,
			'name'=> 'Test',
			'excluded'=> 'string',
		];
		$exclude_keys = ['excluded'];
		$include_keys = ExtendedPdo::excludeKeys($values, $exclude_keys);
		$this->assertEquals(['id', 'name'], $include_keys);
	}

	public function testExcludeKeysIncludeAll(){
		$values = [
			'id'=> 1,
			'name'=> 'Test',
			'excluded'=> 'string',
		];
		$exclude_keys = [];
		$include_keys = ExtendedPdo::excludeKeys($values, $exclude_keys);
		$this->assertEquals(['id', 'name', 'excluded'], $include_keys);
	}

	public function testDsnDefault(){
		$extended_pdo = new ExtendedPdo('test', 'user', 'pass');
		$dsn = $extended_pdo->getDsn();
		$this->assertEquals('mysql:host=localhost;dbname=test', $dsn);
	}

	public function testDsnCustom(){
		$extended_pdo = new ExtendedPdo('test', 'user', 'pass', [], 'sqlite', 'server.com');
		$dsn = $extended_pdo->getDsn();
		$this->assertEquals('sqlite:host=server.com;dbname=test', $dsn);
	}

	public function testDeleteQuery(){
		$id = 1;
		list($query, $where) = ExtendedPdo::deleteQuery('table', ['id'=> $id], 0);
		$this->assertEquals("DELETE FROM `table` WHERE id = :id", $query);
		$this->assertEquals(['id'=> $id], $where);
	}

	public function testDeleteQueryWithLimit(){
		$id = 1;
		$limit = 2;
		list($query, $where) = ExtendedPdo::deleteQuery('table', ['id'=> $id], $limit);
		$this->assertEquals("DELETE FROM `table` WHERE id = :id LIMIT :_limit", $query);
		$this->assertEquals(['id'=> $id, '_limit'=> $limit], $where);
	}

	public function testDeleteQueryNoWhere(){
		try {
			list($query, $where) = ExtendedPdo::deleteQuery('table', [], 0);
			$this->assertTrue(false, 'The ExtendedPdoException was not thrown');
		}
		catch (ExtendedPdoException $e){
			$this->assertTrue(true);
		}
	}
}
