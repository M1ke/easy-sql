# Easy SQL

An extension to [Aura.Sql](https://github.com/aura/sql) which adds extra database manipulation methods, extra fetch methods and a PersistendPdo class for long-lived connections as part of event driven applications.

### Use

```php
$pdo = new \M1ke\Sql\ExtendedPdo
$user_id = $pdo->queryInsert('users', ['name'=>'Foo', 'email'=>'foo@bar.com']); // user created, returns ID
$pdo->queryUpdate('users', "SET :params WHERE user_id={$user_id}", ['name'=>'Bar']); // user name changed to "Bar"
```

For real time applications simply run methods on the static `PersistPdo` object:

```php
PersistPdo::setConfig('database', 'user', 'pass');
PersistPdo::fetchOne("SELECT * FROM users WHERE user_id = 1");
// returns ['user_id'=>1, name'=>'Bar', 'email'=>'foo@bar.com']
```
