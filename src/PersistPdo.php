<?php
namespace M1ke\Sql;

use PDO as PDO;
use PDOException as PDOException;

/**
 *
 * Enables a static instance of PDO to be maintained in a persistent mode
 * with the main use case being for event driven applications
 *
 * @package M1ke.Sql
 *
 */
class PersistPdo {
	/**
	 * @var ExtendedPdo
	 */
	private static $db;
	/**
	 * @var bool
	 */
	private static $allow_reset = true;
	/**
	 * @var array
	 */
	private static $log = [];
	/**
	 * @var array
	 */
	private static $config = [];

	/**
	 * @var string
	 */
	protected static $class = '\\M1ke\\Sql\\ExtendedPdo';
	/**
	 * @var array
	 */
	protected static $attributes = [
		PDO::ATTR_PERSISTENT => true,
	];

	public static function setClass($class_name){
		self::$class = $class_name;
	}

	public static function setAttribute($name, $value){
		self::$attributes[$name] = $value;
	}

	public static function setConfig($db, $user, $pass, $type = 'mysql', $server = 'localhost'){
		self::$config = [
			'db' => $db,
			'user' => $user,
			'pass' => $pass,
			'type' => $type,
			'server' => $server,
		];
	}

	protected static function log($msg, $error = false){
		self::$log[] = $msg;
		if ($error){
			throw new PDOException($msg);
		}
	}

	/**
	 * @return ExtendedPdo
	 */
	public static function getInstance(){
		if (self::$db instanceof PDO){
			self::log('Return static instance');

			return self::$db;
		}
		try {
			self::log('Create new instance');

			$class_name = self::$class;
			$config = self::$config;

			$db = new $class_name($config['db'], $config['user'], $config['pass'],
				self::$attributes, $config['type'], $config['server']);

			if ($db instanceof PDO===false){
				self::log('No PDO object could be created.', true);
			}
		}
		catch (PDOException $e) {
			self::log('PDO encountered an error when connecting: ' . $e->getMessage(), true);
		}

		return (self::$db = $db);
	}

	public static function __callStatic($name, $args){
		$db = self::getInstance();
		try {
			$result = $db->$name(...$args);
			self::$allow_reset = true;

			return $result;
		}
		catch (PDOException $e) {
			$error = $e->getMessage();
			if (self::$allow_reset && self::_inString('MySQL server has gone away', $error)){
				self::reset();
				self::$allow_reset = false;
				self::$name(...$args);
			}
			self::log('PDO encountered an error when executing "' . $name . '": ' . $error, true);
		}

		return false;
	}

	public static function reset(){
		self::$db = null;
	}

	// Taken from m1ke/easy-site-utils
	private static function _inString($needle, $haystack){
		return (stripos($haystack, $needle)!==false);
	}
}
