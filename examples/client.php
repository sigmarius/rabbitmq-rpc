<?php

declare(strict_types=1);

/**
 * Пример RPC-клиента: синхронный вызов и асинхронный batch.
 * Запуск: php examples/client.php
 * Сначала запустите server.php.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Rpc\RpcClient;

$client = new RpcClient('localhost', 5672, 'guest', 'guest');
$client->setTimeout(5.0);

echo "=== Синхронный вызов (getUsers.rpc) ===\n";
$response = $client->call('x_topic', 'getUsers.rpc', ['login' => 'john13']);
echo "Response: " . $response . "\n\n";

echo "=== Асинхронный batch: несколько запросов за раз ===\n";
$requests = [];
for ($i = 0; $i < 10; $i++) {
    if ($i % 2 === 0) {
        $requests["req_user_$i"] = [
            'routing_key' => 'getUsers.rpc',
            'body'        => ['login' => 'john13'],
        ];
    } else {
        $requests["req_by_id_$i"] = [
            'routing_key' => 'user.getById.rpc',
            'body'        => ['user_id' => $i],
        ];
    }
}
$results = $client->callBatch('x_topic', $requests);
foreach ($results as $requestId => $body) {
    echo $requestId . " => " . $body . "\n";
}

$client->close();
echo "\nDone.\n";
