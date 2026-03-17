<?php

declare(strict_types=1);

/**
 * Пример RPC-сервера: обрабатывает запросы getUsers.rpc и user.getById.rpc.
 * Запуск: из корня 9.RPC выполнить: php examples/server.php
 * Требуется запущенный RabbitMQ (например Docker: rabbitmq:3.10-management на localhost:5672).
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Rpc\RpcServer;

// Эмуляция БД пользователей по логину (как в теоретическом материале)
$users = [
    'john13' => ['name' => 'John Evelinne', 'born' => 1992, 'city' => 'New York'],
    'pink_kitty' => ['name' => 'Alice', 'born' => 1995, 'city' => 'London'],
    'h4x0r' => ['name' => 'Bob', 'born' => 1988, 'city' => 'Berlin'],
];

$server = new RpcServer('localhost', 5672, 'guest', 'guest', 'q_rpc');
$server->setRequestLogger(function (string $routingKey, string $replyTo, string $correlationId, string $body) {
    $snippet = strlen($body) > 80 ? substr($body, 0, 80) . '...' : $body;
    echo "[RPC] routing_key={$routingKey} reply_to=" . (strlen($replyTo) ? 'yes' : 'no') . " body={$snippet}\n";
});
$server->bindExchange('x_topic', '#.rpc');

// Обработчик: получить пользователя по логину (getUsers.rpc)
$server->handle('getUsers.rpc', function ($payload) use ($users) {
    $login = is_array($payload) ? ($payload['login'] ?? '') : $payload;
    if ($login === '') {
        return ['error' => 'login required'];
    }
    $user = $users[$login] ?? null;
    if ($user === null) {
        return ['error' => 'user not found', 'login' => $login];
    }
    return array_merge(['login' => $login], $user);
});

// Обработчик: получить пользователя по id (user.getById.rpc)
$server->handle('user.getById.rpc', function ($payload) use ($users) {
    $id = is_array($payload) ? (int)($payload['user_id'] ?? 0) : (int)$payload;
    $logins = array_keys($users);
    $login = $logins[$id % count($logins)] ?? null;
    if ($login === null) {
        return ['error' => 'user not found', 'user_id' => $id];
    }
    $user = $users[$login];
    return array_merge(['login' => $login], $user);
});

echo "RPC Server started. Queue: q_rpc, Exchange: x_topic. Waiting for requests...\n";
$server->run();
