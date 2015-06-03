# Easy SQL

An extension to [Aura.Sql](https://github.com/auraphp/Aura.Sql) which simplifies object creation, adds extra database manipulation methods, extra fetch methods and a PersistendPdo class for long-lived connections as part of event driven applications.

### Use

```php
use M1ke\Sql\ExtendedPdo;

$pdo = new ExtendedPdo('database', 'user', 'pass');
$user_id = $pdo->queryInsert('users', ['name'=>'Foo', 'email'=>'foo@bar.com']);
// user created, returns ID
$affected_rows = $pdo->queryUpdate('users', "SET :params WHERE user_id={$user_id}", ['name'=>'Bar']);
// user name changed to "Bar", returns number of rows affected
```

For real time applications simply run methods on the static `PersistPdo` object:

```php
use M1ke\Sql\PersistPdo;

PersistPdo::setConfig('database', 'user', 'pass');
PersistPdo::fetchOne("SELECT * FROM users WHERE user_id = 1");
// returns ['user_id'=>1, name'=>'Bar', 'email'=>'foo@bar.com']
```
