<?php

require __DIR__.'/../src/ExtendedPdo.php';
require __DIR__.'/../vendor/autoload.php';

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
		$this->assertEquals("UPDATE table SET `string`=:string WHERE key = :key", $query);
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
		$this->assertEquals("UPDATE table SET `string`=:string WHERE key=1", $query);
		$this->assertEquals([
			'string'=> 'test',
		], $values);
	}
}